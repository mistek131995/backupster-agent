using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup.MssqlLogicalBackup;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Dac;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlLogicalBackupProvider(ILogger<MssqlLogicalBackupProvider> logger) : IBackupProvider
{
    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        await ValidateBackupPermissionsAsync(connection, database, ct);
        await ValidateDacFxCompatibilityAsync(connection, database, ct);
    }

    private static async Task ValidateBackupPermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        const string sql = @"
SELECT IS_MEMBER('db_owner')     AS is_owner,
       IS_MEMBER('db_datareader') AS is_datareader,
       HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION')      AS can_view_def,
       HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DATABASE STATE')  AS can_view_state;";

        await using var conn = new SqlConnection(MssqlConnectionFactory.BuildDatabaseConnectionString(connection, database));
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        if (!await reader.ReadAsync(ct))
            throw new BackupPermissionException("Не удалось прочитать данные о правах пользователя.");

        var isOwner      = reader.GetInt32(0) == 1;
        var isDatareader = reader.GetInt32(1) == 1;
        var canViewDef   = reader.GetInt32(2) == 1;
        var canViewState = reader.GetInt32(3) == 1;

        if (isOwner || (isDatareader && canViewDef && canViewState)) return;

        var member = MssqlConnectionFactory.GrantMemberName(connection);
        throw new BackupPermissionException(
            $"{MssqlConnectionFactory.DescribeUser(connection)} не имеет прав для logical бэкапа БД '{database}'. " +
            "Требуется членство в db_owner, либо одновременно: db_datareader + VIEW DEFINITION + VIEW DATABASE STATE. " +
            $"Пример: ALTER ROLE db_datareader ADD MEMBER [{member}]; " +
            $"GRANT VIEW DEFINITION TO [{member}]; " +
            $"GRANT VIEW DATABASE STATE TO [{member}];");
    }

    private async Task ValidateDacFxCompatibilityAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        try
        {
            var result = await InspectDacFxCompatibilityAsync(connection, database, ct);

            if (result.HasWarningFindings)
                logger.LogWarning("{Message}", result.BuildWarningLogMessage(database));

            if (result.HasBlockingFindings)
                throw new BackupUserFacingException(result.BuildBlockingUserMessage(database));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (BackupUserFacingException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "MSSQL logical DacFx preflight failed for database '{Database}', continuing to DacFx export",
                database);
        }
    }

    private static async Task<MssqlLogicalPreflightResult> InspectDacFxCompatibilityAsync(
        ConnectionConfig connection,
        string database,
        CancellationToken ct)
    {
        const string sql = @"
DECLARE @db sysname = DB_NAME();
DECLARE @findings TABLE
(
    Code nvarchar(64) NOT NULL,
    Detail nvarchar(4000) NULL
);

INSERT INTO @findings (Code, Detail)
SELECT DISTINCT
    N'LinkedServerReference',
    COALESCE(QUOTENAME(OBJECT_SCHEMA_NAME(referencing_id)) + N'.' + QUOTENAME(OBJECT_NAME(referencing_id)), N'object_id=' + CONVERT(nvarchar(20), referencing_id)) +
    N' -> ' + QUOTENAME(referenced_server_name) +
    COALESCE(N'.' + QUOTENAME(referenced_database_name), N'') +
    COALESCE(N'.' + QUOTENAME(referenced_schema_name), N'') +
    COALESCE(N'.' + QUOTENAME(referenced_entity_name), N'')
FROM sys.sql_expression_dependencies
WHERE NULLIF(LTRIM(RTRIM(referenced_server_name)), N'') IS NOT NULL;

INSERT INTO @findings (Code, Detail)
SELECT DISTINCT
    N'CrossDatabaseReference',
    COALESCE(QUOTENAME(OBJECT_SCHEMA_NAME(referencing_id)) + N'.' + QUOTENAME(OBJECT_NAME(referencing_id)), N'object_id=' + CONVERT(nvarchar(20), referencing_id)) +
    N' -> ' + QUOTENAME(referenced_database_name) +
    COALESCE(N'.' + QUOTENAME(referenced_schema_name), N'') +
    COALESCE(N'.' + QUOTENAME(referenced_entity_name), N'')
FROM sys.sql_expression_dependencies
WHERE NULLIF(LTRIM(RTRIM(referenced_database_name)), N'') IS NOT NULL
  AND referenced_database_name COLLATE DATABASE_DEFAULT <> @db COLLATE DATABASE_DEFAULT;

INSERT INTO @findings (Code, Detail)
SELECT N'CdcEnabled', N'database flag'
FROM sys.databases
WHERE database_id = DB_ID()
  AND is_cdc_enabled = 1;

INSERT INTO @findings (Code, Detail)
SELECT N'CdcEnabled', QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(name)
FROM sys.tables
WHERE is_tracked_by_cdc = 1;

INSERT INTO @findings (Code, Detail)
SELECT N'ReplicationEnabled', QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(name)
FROM sys.tables
WHERE is_replicated = 1
   OR has_replication_filter = 1
   OR is_merge_published = 1
   OR is_sync_tran_subscribed = 1;

INSERT INTO @findings (Code, Detail)
SELECT N'FileStream', QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) + N'.' + QUOTENAME(c.name)
FROM sys.columns c
INNER JOIN sys.tables t ON t.object_id = c.object_id
WHERE c.is_filestream = 1;

INSERT INTO @findings (Code, Detail)
SELECT N'FileStream', QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(name) + N' (FileTable)'
FROM sys.tables
WHERE is_filetable = 1;

INSERT INTO @findings (Code, Detail)
SELECT N'FileStream', N'filegroup ' + QUOTENAME(name)
FROM sys.filegroups
WHERE type = N'FD';

INSERT INTO @findings (Code, Detail)
SELECT N'ClrUnsafeOrExternal', QUOTENAME(name) + N' (' + permission_set_desc + N')'
FROM sys.assemblies
WHERE is_user_defined = 1
  AND permission_set_desc IN (N'EXTERNAL_ACCESS', N'UNSAFE_ACCESS');

INSERT INTO @findings (Code, Detail)
SELECT N'OrphanedUser', QUOTENAME(name)
FROM sys.database_principals
WHERE type IN (N'S', N'U', N'G')
  AND name NOT IN (N'dbo', N'guest', N'sys', N'INFORMATION_SCHEMA')
  AND authentication_type_desc NOT IN (N'DATABASE', N'NONE')
  AND sid IS NOT NULL
  AND sid <> 0x00
  AND SUSER_SNAME(sid) IS NULL;

INSERT INTO @findings (Code, Detail)
SELECT N'TdeEnabled', N'database encryption is enabled'
FROM sys.databases
WHERE database_id = DB_ID()
  AND is_encrypted = 1;

SELECT Code, Detail
FROM @findings
ORDER BY Code, Detail;";

        var findings = new List<MssqlLogicalPreflightResult.Finding>();

        await using var conn = new SqlConnection(MssqlConnectionFactory.BuildDatabaseConnectionString(connection, database));
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var code = reader.GetString(0);
            var detail = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            findings.Add(MssqlLogicalPreflightResult.CreateFinding(code, detail));
        }

        return new MssqlLogicalPreflightResult(findings);
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.DatabasePathSegment}_{timestamp}.bacpac";
        var outputFile = Path.Combine(config.OutputPath, fileName);

        Directory.CreateDirectory(config.OutputPath);

        var connectionString = MssqlConnectionFactory.BuildDatabaseConnectionString(connection, config.Database);
        var dataSource = MssqlConnectionFactory.DescribeDataSource(connection);

        logger.LogInformation(
            "Starting MSSQL logical backup. Database: '{Database}', DataSource: '{DataSource}', Output: '{Output}'",
            config.Database, dataSource, outputFile);

        var dac = new DacServices(connectionString);
        var dacErrors = new List<DacMessage>();
        void OnDacMessage(object? sender, DacMessageEventArgs e)
        {
            CaptureDacError(dacErrors, e.Message);
            LogDacMessage(e.Message);
        }

        dac.Message += OnDacMessage;
        dac.ProgressChanged += OnDacProgress;

        var sw = Stopwatch.StartNew();
        try
        {
            await Task.Run(
                () => dac.ExportBacpac(outputFile, config.Database, cancellationToken: ct),
                ct);
        }
        catch (OperationCanceledException)
        {
            TryDeleteFile(outputFile);
            throw;
        }
        catch (DacServicesException ex)
        {
            TryDeleteFile(outputFile);
            logger.LogError(ex, "DacFx ExportBacpac failed for database '{Database}'", config.Database);
            throw new BackupUserFacingException(
                MssqlDacFxErrorFormatter.BuildExportFailureMessage(config.Database, ex, dacErrors),
                ex);
        }
        finally
        {
            dac.Message -= OnDacMessage;
            dac.ProgressChanged -= OnDacProgress;
        }

        sw.Stop();

        if (!File.Exists(outputFile))
        {
            throw new InvalidOperationException(
                $"Файл bacpac '{outputFile}' не создан DacFx, хотя операция завершилась без ошибок.");
        }

        var sizeBytes = new FileInfo(outputFile).Length;

        logger.LogInformation(
            "MSSQL logical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
            outputFile, sizeBytes, sw.ElapsedMilliseconds);

        return new BackupResult
        {
            FilePath = outputFile,
            SizeBytes = sizeBytes,
            DurationMs = sw.ElapsedMilliseconds,
            Success = true,
        };
    }

    private static void CaptureDacError(List<DacMessage> dacErrors, DacMessage message)
    {
        if (message.MessageType != DacMessageType.Error)
            return;

        if (dacErrors.Any(existing =>
                existing.Number == message.Number &&
                string.Equals(existing.Message, message.Message, StringComparison.Ordinal)))
            return;

        if (dacErrors.Count < 20)
            dacErrors.Add(message);
    }

    private void LogDacMessage(DacMessage msg)
    {
        switch (msg.MessageType)
        {
            case DacMessageType.Error:
                logger.LogError("DacFx Error {Number}: {Message}", msg.Number, msg.Message);
                break;
            case DacMessageType.Warning:
                logger.LogWarning("DacFx Warning {Number}: {Message}", msg.Number, msg.Message);
                break;
            default:
                logger.LogDebug("DacFx Message {Number}: {Message}", msg.Number, msg.Message);
                break;
        }
    }

    private void OnDacProgress(object? sender, DacProgressEventArgs e)
    {
        logger.LogDebug("DacFx Progress: Status={Status}, Message={Message}", e.Status, e.Message);
    }

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { logger.LogWarning(ex, "Could not delete partial file '{Path}'", path); }
    }

}
