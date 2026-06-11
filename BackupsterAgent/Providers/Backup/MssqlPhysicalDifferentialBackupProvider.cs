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
    private static readonly int[] PermissionErrorNumbers = { 229, 262, 300, 916, 15247, 21089 };

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
            throw new BackupUserFacingException(
                $"Для БД '{config.Database}' не задан OutputPath. " +
                "Для MSSQL differential backup укажите Databases[].OutputPath - каталог, доступный агенту и SQL Server.");
        }

        var outputPath = Path.GetFullPath(config.OutputPath);
        Directory.CreateDirectory(outputPath);

        var sqlFilePath = Path.Combine(outputPath, fileName);
        var agentFilePath = sqlFilePath;
        var dataSource = MssqlConnectionFactory.DescribeDataSource(connection);

        _logger.LogInformation(
            "Starting MSSQL differential backup. Database: '{Database}', DataSource: '{DataSource}', " +
            "SQL path: '{SqlPath}', Agent path: '{AgentPath}', BaseRecordId: '{BaseRecordId}'",
            config.Database, dataSource, sqlFilePath, agentFilePath, context.BaseBackupRecordId);

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
                throw new BackupUserFacingException(
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
        catch (SqlException ex)
        {
            TryDeleteFile(agentFilePath);
            throw BuildSqlBackupException(ex, config, connection, sqlFilePath);
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

            throw new BackupUserFacingException(
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

            case MssqlDifferentialChainCheck.BaseUnknownOrAmbiguous:
                _logger.LogWarning(
                    "MSSQL differential chain check refused for '{Database}': current database differential base is unknown or ambiguous.",
                    database);

                throw new DifferentialChainBrokenException(
                    $"Дифференциальный бэкап БД '{database}' отменён: SQL Server не смог однозначно определить текущую базу для дифференциального бэкапа. " +
                    "Такое возможно, если полный бэкап для этой БД ещё не снимался, база была восстановлена частично или файлы данных опираются на разные базовые бэкапы. " +
                    "Запускаем новый полный бэкап автоматически, чтобы восстановить цепочку.");

            case MssqlDifferentialChainCheck.BaseDiverged:
                _logger.LogWarning(
                    "MSSQL differential chain check refused for '{Database}': current database differential base does not match parent FULL file '{ParentFile}'.",
                    database, parentBackupFileName);

                throw new DifferentialChainBrokenException(
                    $"Дифференциальный бэкап БД '{database}' отменён: текущая база SQL Server для дифференциального бэкапа не совпадает с полным бэкапом Backupster, который выбрал дашборд. " +
                    "Скорее всего, БД была восстановлена в более старую точку или цепочка была изменена вне Backupster. " +
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
                throw new BackupUserFacingException(
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

        throw new BackupUserFacingException(
            $"Дифференциальный бэкап БД '{database}' отменён: не удалось получить эксклюзивную блокировку цепочки MSSQL в течение {DifferentialChainLockTimeoutMs / 1000} секунд. " +
            "Вероятно, для этой БД уже выполняется другой бэкап Backupster. Повторите запуск позже.");
    }

    private static Exception BuildSqlBackupException(
        SqlException ex,
        DatabaseConfig config,
        ConnectionConfig connection,
        string sqlFilePath)
    {
        if (HasError(ex, PermissionErrorNumbers))
        {
            return new BackupPermissionException(
                $"{MssqlConnectionFactory.DescribeUser(connection)} не имеет прав для MSSQL differential backup БД '{config.Database}'. " +
                "Требуются права BACKUP DATABASE или членство в роли db_backupoperator, db_owner либо sysadmin.",
                ex);
        }

        if (HasError(ex, 3035))
        {
            return new BackupUserFacingException(
                $"MSSQL отказался создать дифференциальный бэкап БД '{config.Database}': у базы нет предыдущего полного бэкапа в msdb. " +
                "Запустите сначала полный бэкап.",
                ex);
        }

        if (HasError(ex, 911))
        {
            return new BackupUserFacingException(
                $"БД '{config.Database}' не найдена на MSSQL-сервере подключения '{connection.Name}'. Проверьте имя БД в Databases[].Database.",
                ex);
        }

        if (HasError(ex, 924, 927, 942, 945, 952))
        {
            return new BackupUserFacingException(
                $"БД '{config.Database}' недоступна для MSSQL differential backup. Проверьте, что база online и файлы БД доступны SQL Server.",
                ex);
        }

        if (HasError(ex, 3201, 3202, 18204, 18210))
        {
            return new BackupUserFacingException(
                $"SQL Server не смог записать файл дифференциального бэкапа по пути '{sqlFilePath}'. " +
                "Проверьте, что Databases[].OutputPath виден SQL Server и у service account SQL Server есть права на запись.",
                ex);
        }

        if (HasError(ex, 3013, 3041))
        {
            return new BackupUserFacingException(
                $"MSSQL не смог создать differential backup БД '{config.Database}'. Проверьте права, состояние БД и доступность Databases[].OutputPath для SQL Server.",
                ex);
        }

        return new BackupUserFacingException(
            $"MSSQL не смог создать differential backup БД '{config.Database}'. Подробности смотрите в логах агента.",
            ex);
    }

    private static bool HasError(SqlException ex, params int[] errorNumbers)
    {
        foreach (SqlError error in ex.Errors)
        {
            if (Array.IndexOf(errorNumbers, error.Number) >= 0)
                return true;
        }

        return false;
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
