using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlPhysicalDifferentialBackupProvider : IDifferentialBackupProvider
{
    private readonly ILogger<MssqlPhysicalDifferentialBackupProvider> _logger;
    private readonly MssqlPhysicalBackupProvider _fullProvider;

    public MssqlPhysicalDifferentialBackupProvider(
        ILogger<MssqlPhysicalDifferentialBackupProvider> logger,
        MssqlPhysicalBackupProvider fullProvider)
    {
        _logger = logger;
        _fullProvider = fullProvider;
    }

    public Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct) =>
        _fullProvider.ValidatePermissionsAsync(connection, database, ct);

    public async Task<BackupResult> BackupAsync(
        DatabaseConfig config,
        ConnectionConfig connection,
        DifferentialBackupContext context,
        CancellationToken ct)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}_diff.bak";

        var sqlDir = await MssqlSharedPathResolver.GetSqlDirAsync(connection, ct);
        var agentDir = await MssqlSharedPathResolver.GetAgentDirAsync(connection, ct);

        Directory.CreateDirectory(agentDir);

        var sqlFilePath = MssqlSharedPathResolver.JoinSqlPath(sqlDir, fileName);
        var agentFilePath = Path.Combine(agentDir, fileName);

        _logger.LogInformation(
            "Starting MSSQL differential backup. Database: '{Database}', Host: '{Host}:{Port}', " +
            "SQL path: '{SqlPath}', Agent path: '{AgentPath}', BaseRecordId: '{BaseRecordId}'",
            config.Database, connection.Host, connection.Port, sqlFilePath, agentFilePath, context.BaseBackupRecordId);

        await EnsureNoForeignFullSinceBaseAsync(connection, config.Database, context.BaseBackupAt, ct);

        var escapedDb = config.Database.Replace("]", "]]");
        var escapedPath = sqlFilePath.Replace("'", "''");
        var tsql = $"BACKUP DATABASE [{escapedDb}] TO DISK = N'{escapedPath}' WITH DIFFERENTIAL, FORMAT, INIT, STATS = 10;";

        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = "master",
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ConnectionString;

        var sw = Stopwatch.StartNew();

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(tsql, conn) { CommandTimeout = 0 };
        cmd.StatementCompleted += (_, e) =>
            _logger.LogDebug("MSSQL differential backup progress: {RecordsAffected} rows affected", e.RecordCount);

        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "MSSQL differential backup cancelled for '{Database}', attempting cleanup of '{Path}'",
                config.Database, agentFilePath);
            TryDeleteFile(agentFilePath);
            throw;
        }
        catch (SqlException ex) when (ex.Number == 3035)
        {
            throw new InvalidOperationException(
                $"MSSQL отказался создать дифференциальный бэкап БД '{config.Database}': " +
                "у базы нет ни одного предыдущего полного бэкапа (msdb о нём не знает). " +
                "Возможно, полный бэкап был сделан другим инструментом или удалён напрямую из msdb. " +
                "Запустите сначала полный бэкап.", ex);
        }

        sw.Stop();

        if (!File.Exists(agentFilePath))
        {
            throw new InvalidOperationException(
                $"Файл дифференциального бэкапа '{agentFilePath}' недоступен на хосте агента. " +
                "Проверьте, что SharedBackupPath и AgentBackupPath указывают на один и тот же каталог.");
        }

        var sizeBytes = new FileInfo(agentFilePath).Length;

        _logger.LogInformation(
            "MSSQL differential backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
            agentFilePath, sizeBytes, sw.ElapsedMilliseconds);

        return new BackupResult
        {
            FilePath = agentFilePath,
            SizeBytes = sizeBytes,
            DurationMs = sw.ElapsedMilliseconds,
            Success = true,
        };
    }

    private async Task EnsureNoForeignFullSinceBaseAsync(
        ConnectionConfig connection,
        string database,
        DateTime? baseBackupAt,
        CancellationToken ct)
    {
        if (baseBackupAt is null)
        {
            _logger.LogWarning(
                "MSSQL differential chain check refused for '{Database}': dashboard did not provide BaseBackupAt. " +
                "Cannot detect external FULL backups that would re-base the differential chain.",
                database);

            throw new InvalidOperationException(
                $"Дифференциальный бэкап БД '{database}' отменён: дашборд не передал отметку времени родительского полного бэкапа, " +
                "поэтому невозможно проверить, что цепочка восстановления не была перебита сторонним полным бэкапом. " +
                "Обновите дашборд до версии 1.4.0 или выше, либо запустите новый полный бэкап через Backupster.");
        }

        const string sql = @"
SELECT COUNT(*) FROM msdb.dbo.backupset
WHERE database_name = @db
  AND type = 'D'
  AND is_copy_only = 0
  AND DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), backup_finish_date) >
      DATEADD(SECOND, 60, @baseUtc);";

        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = "msdb",
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ConnectionString;

        int foreignCount;
        await using (var conn = new SqlConnection(connectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
            cmd.Parameters.Add(new SqlParameter("@db", System.Data.SqlDbType.NVarChar, 128) { Value = database });
            cmd.Parameters.Add(new SqlParameter("@baseUtc", System.Data.SqlDbType.DateTime2) { Value = baseBackupAt.Value });
            foreignCount = (int)(await cmd.ExecuteScalarAsync(ct) ?? 0);
        }

        if (foreignCount > 0)
        {
            _logger.LogError(
                "MSSQL differential chain check failed for '{Database}': {Count} foreign FULL backup(s) detected in msdb after BaseBackupAt={BaseAt:o}.",
                database, foreignCount, baseBackupAt.Value);

            throw new InvalidOperationException(
                $"Дифференциальный бэкап БД '{database}' отменён: в msdb обнаружен(ы) сторонний(е) полный(е) бэкап(ы), " +
                "сделанные после родительского полного бэкапа Backupster. SQL Server привязал бы DIFF к стороннему FULL, " +
                "а Backupster ожидал бы свой — цепочка восстановления была бы повреждена. " +
                "Запустите новый полный бэкап через Backupster, чтобы восстановить цепочку.");
        }
    }

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial backup file '{Path}'", path); }
    }
}
