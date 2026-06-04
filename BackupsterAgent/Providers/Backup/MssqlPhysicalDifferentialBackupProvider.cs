using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlPhysicalDifferentialBackupProvider : IDifferentialBackupProvider
{
    private const int DifferentialChainLockTimeoutMs = 60_000;

    private readonly ILogger<MssqlPhysicalDifferentialBackupProvider> _logger;
    private readonly MssqlPhysicalBackupProvider _fullProvider;
    private readonly MssqlDifferentialChainGuard _chainGuard;

    public MssqlPhysicalDifferentialBackupProvider(
        ILogger<MssqlPhysicalDifferentialBackupProvider> logger,
        MssqlPhysicalBackupProvider fullProvider,
        MssqlDifferentialChainGuard chainGuard)
    {
        _logger = logger;
        _fullProvider = fullProvider;
        _chainGuard = chainGuard;
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
        var fileName = $"{config.DatabasePathSegment}_{timestamp}_diff.bak";

        if (string.IsNullOrWhiteSpace(config.OutputPath))
        {
            throw new InvalidOperationException(
                $"Для БД '{config.Database}' не задан OutputPath. " +
                "Для MSSQL differential backup укажите Databases[].OutputPath — каталог, доступный агенту и SQL Server.");
        }

        var outputPath = Path.GetFullPath(config.OutputPath);
        Directory.CreateDirectory(outputPath);

        var sqlFilePath = Path.Combine(outputPath, fileName);
        var agentFilePath = sqlFilePath;

        _logger.LogInformation(
            "Starting MSSQL differential backup. Database: '{Database}', Host: '{Host}:{Port}', " +
            "SQL path: '{SqlPath}', Agent path: '{AgentPath}', BaseRecordId: '{BaseRecordId}'",
            config.Database, connection.Host, connection.Port, sqlFilePath, agentFilePath, context.BaseBackupRecordId);

        var escapedDb = config.Database.Replace("]", "]]");
        var escapedPath = sqlFilePath.Replace("'", "''");
        var tsql = $"BACKUP DATABASE [{escapedDb}] TO DISK = N'{escapedPath}' WITH DIFFERENTIAL, FORMAT, INIT, STATS = 10;";

        var connectionString = MssqlConnectionFactory.BuildMasterConnectionString(connection);

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);

        await AcquireDifferentialChainLockAsync(conn, config.Database, ct);
        await EnsureNoForeignFullSinceBaseAsync(
            conn,
            config.Database,
            context.BaseDumpObjectKey,
            ct);

        var sw = Stopwatch.StartNew();

        await using var cmd = new SqlCommand(tsql, conn) { CommandTimeout = 0 };
        cmd.StatementCompleted += (_, e) =>
            _logger.LogDebug("MSSQL differential backup progress: {RecordsAffected} rows affected", e.RecordCount);

        try
        {
            await cmd.ExecuteNonQueryAsync(ct);

            ct.ThrowIfCancellationRequested();

            sw.Stop();

            if (!File.Exists(agentFilePath))
            {
                throw new InvalidOperationException(
                    $"Файл дифференциального бэкапа '{agentFilePath}' недоступен на хосте агента. " +
                    "Проверьте, что OutputPath указывает на каталог, доступный агенту и SQL Server.");
            }

            var sizeBytes = new FileInfo(agentFilePath).Length;

            ct.ThrowIfCancellationRequested();

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
    }

    private async Task EnsureNoForeignFullSinceBaseAsync(
        SqlConnection conn,
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
                $"Дифференциальный бэкап БД '{database}' отменён: нет предыдущего полного бэкапа, на который можно опереться, — дашборд не передал данные о родительском полном бэкапе, и нельзя убедиться, что цепочка не сломана. " +
                "Обновите дашборд до версии 1.4.0 или новее.");
        }

        var checkResult = await _chainGuard.InspectAsync(conn, database, parentBackupFileName, ct);

        switch (checkResult)
        {
            case MssqlDifferentialChainCheck.Ok:
                return;

            case MssqlDifferentialChainCheck.ParentMissing:
                _logger.LogWarning(
                    "MSSQL differential chain check refused for '{Database}': parent FULL file '{ParentFile}' not found in msdb.",
                    database, parentBackupFileName);

                throw new DifferentialChainBrokenException(
                    $"Дифференциальный бэкап БД '{database}' отменён: на SQL Server пропала запись о родительском полном бэкапе Backupster. " +
                    "Скорее всего, историю бэкапов на сервере очистил maintenance plan или DBA — теперь нельзя проверить, что цепочка не сломалась. " +
                    "Запускаем новый полный бэкап автоматически, чтобы восстановить цепочку.");

            case MssqlDifferentialChainCheck.ForeignFullDetected:
                _logger.LogError(
                    "MSSQL differential chain check failed for '{Database}': foreign full-like backup(s) detected in msdb after parent file '{ParentFile}'.",
                    database, parentBackupFileName);

                throw new DifferentialChainBrokenException(
                    $"Дифференциальный бэкап БД '{database}' отменён: на SQL Server обнаружен полный бэкап этой БД, сделанный не через Backupster, — " +
                    "его создал другой инструмент (например, штатный SQL Server maintenance plan, Veeam или DBA вручную). " +
                    "При попытке восстановления цепочка бы порвалась. " +
                    "Запускаем новый полный бэкап автоматически, чтобы восстановить цепочку.");

            default:
                throw new InvalidOperationException(
                    $"Дифференциальный бэкап БД '{database}' отменён: неизвестный результат проверки цепочки MSSQL ({checkResult}).");
        }
    }

    private async Task AcquireDifferentialChainLockAsync(SqlConnection conn, string database, CancellationToken ct)
    {
        const string sql = @"
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = @resource,
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = @timeoutMs;
SELECT @result;";

        var resource = $"Backupster:MSSQL:DIFF:{database}";

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
        cmd.Parameters.Add(new SqlParameter("@resource", System.Data.SqlDbType.NVarChar, 255) { Value = resource });
        cmd.Parameters.Add(new SqlParameter("@timeoutMs", System.Data.SqlDbType.Int) { Value = DifferentialChainLockTimeoutMs });

        var resultObj = await cmd.ExecuteScalarAsync(ct);
        var result = resultObj is int value ? value : -999;
        if (result >= 0)
        {
            _logger.LogDebug(
                "MSSQL differential chain app lock acquired for '{Database}'. Resource: '{Resource}', Result: {Result}",
                database, resource, result);
            return;
        }

        _logger.LogWarning(
            "MSSQL differential chain app lock refused for '{Database}'. Resource: '{Resource}', Result: {Result}",
            database, resource, result);

        throw new InvalidOperationException(
            $"Дифференциальный бэкап БД '{database}' отменён: не удалось получить эксклюзивную блокировку цепочки MSSQL в течение {DifferentialChainLockTimeoutMs / 1000} секунд. " +
            "Вероятно, для этой БД уже выполняется другой бэкап Backupster. Повторите запуск позже.");
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
