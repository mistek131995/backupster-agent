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

        await EnsureNoForeignFullSinceBaseAsync(
            connection,
            config.Database,
            context.BaseDumpObjectKey,
            ct);

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
        string? baseDumpObjectKey,
        CancellationToken ct)
    {
        var parentBackupFileName = ResolveParentBackupFileName(baseDumpObjectKey);
        if (parentBackupFileName is null)
        {
            _logger.LogWarning(
                "MSSQL differential chain check refused for '{Database}': dashboard did not provide BaseDumpObjectKey. " +
                "Cannot detect external FULL backups that would re-base the differential chain.",
                database);

            throw new InvalidOperationException(
                $"Дифференциальный бэкап БД '{database}' отменён: дашборд не передал ключ файла родительского полного бэкапа, " +
                "поэтому невозможно сопоставить цепочку Backupster с msdb и проверить, что она не была перебита сторонним полным бэкапом. " +
                "Обновите дашборд до версии 1.4.0 или выше, либо запустите новый полный бэкап через Backupster.");
        }

        const string sql = @"
DECLARE @parent_backup_set_id INT;

SELECT TOP (1)
    @parent_backup_set_id = bs.backup_set_id
FROM msdb.dbo.backupset bs
INNER JOIN msdb.dbo.backupmediafamily bmf
    ON bmf.media_set_id = bs.media_set_id
WHERE bs.database_name = @db
  AND bs.type = 'D'
  AND bs.is_copy_only = 0
  AND RIGHT(bmf.physical_device_name, LEN(@parentFileName)) = @parentFileName
ORDER BY bs.backup_set_id DESC;

SELECT
    CASE WHEN @parent_backup_set_id IS NULL THEN 0 ELSE 1 END AS parent_count,
    CASE
        WHEN @parent_backup_set_id IS NULL THEN CAST(0 AS BIGINT)
        ELSE (
            SELECT COUNT_BIG(*)
            FROM msdb.dbo.backupset
            WHERE database_name = @db
              AND type = 'D'
              AND is_copy_only = 0
              AND backup_set_id > @parent_backup_set_id
        )
    END AS foreign_count;";

        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = "msdb",
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ConnectionString;

        int parentCount;
        long foreignCount;
        await using (var conn = new SqlConnection(connectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
            cmd.Parameters.Add(new SqlParameter("@db", System.Data.SqlDbType.NVarChar, 128) { Value = database });
            cmd.Parameters.Add(new SqlParameter("@parentFileName", System.Data.SqlDbType.NVarChar, 260) { Value = parentBackupFileName });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                parentCount = 0;
                foreignCount = 0;
            }
            else
            {
                parentCount = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                foreignCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1);
            }
        }

        if (parentCount == 0)
        {
            _logger.LogWarning(
                "MSSQL differential chain check refused for '{Database}': parent FULL file '{ParentFile}' not found in msdb.",
                database, parentBackupFileName);

            throw new InvalidOperationException(
                $"Дифференциальный бэкап БД '{database}' отменён: в служебной базе msdb отсутствует запись о родительском полном бэкапе. " +
                "Скорее всего, история msdb была очищена системной задачей (sp_delete_backuphistory или maintenance plan), " +
                "поэтому невозможно достоверно проверить, не привязан ли DIFF к стороннему полному бэкапу. " +
                "Запустите новый полный бэкап через Backupster, чтобы восстановить цепочку.");
        }

        if (foreignCount > 0)
        {
            _logger.LogError(
                "MSSQL differential chain check failed for '{Database}': {Count} foreign FULL backup(s) detected in msdb after parent file '{ParentFile}'.",
                database, foreignCount, parentBackupFileName);

            throw new InvalidOperationException(
                $"Дифференциальный бэкап БД '{database}' отменён: в msdb обнаружен(ы) сторонний(е) полный(е) бэкап(ы), " +
                "сделанные после родительского полного бэкапа Backupster. SQL Server привязал бы DIFF к стороннему FULL, " +
                "а Backupster ожидал бы свой — цепочка восстановления была бы повреждена. " +
                "Запустите новый полный бэкап через Backupster, чтобы восстановить цепочку.");
        }
    }

    private static string? ResolveParentBackupFileName(string? baseDumpObjectKey)
    {
        if (string.IsNullOrWhiteSpace(baseDumpObjectKey)) return null;

        var fileName = Path.GetFileName(baseDumpObjectKey.Replace('\\', '/'));
        if (string.IsNullOrWhiteSpace(fileName)) return null;

        return fileName.EndsWith(".enc", StringComparison.OrdinalIgnoreCase)
            ? fileName[..^4]
            : fileName;
    }

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial backup file '{Path}'", path); }
    }
}
