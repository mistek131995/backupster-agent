using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlPhysicalBackupProvider : IBackupProvider
{
    private static readonly int[] PermissionErrorNumbers = { 229, 262, 300, 916, 15247, 21089 };

    private readonly ILogger<MssqlPhysicalBackupProvider> _logger;

    public MssqlPhysicalBackupProvider(ILogger<MssqlPhysicalBackupProvider> logger)
    {
        _logger = logger;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        await ValidateBackupPermissionsAsync(connection, database, ct);
    }

    private static async Task ValidateBackupPermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
    {
        try
        {
            const string sql = @"
SELECT IS_SRVROLEMEMBER('sysadmin')      AS is_sysadmin,
       IS_MEMBER('db_owner')             AS is_owner,
       IS_MEMBER('db_backupoperator')    AS is_backupoperator;";

            await using var conn = new SqlConnection(MssqlConnectionFactory.BuildDatabaseConnectionString(connection, database));
            await conn.OpenAsync(ct);

            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (!await reader.ReadAsync(ct))
                throw new BackupPermissionException("Не удалось прочитать данные о правах пользователя.");

            var isSysadmin       = reader.GetInt32(0) == 1;
            var isOwner          = reader.GetInt32(1) == 1;
            var isBackupOperator = reader.GetInt32(2) == 1;

            if (isSysadmin || isOwner || isBackupOperator) return;

            throw new BackupPermissionException(
                $"{MssqlConnectionFactory.DescribeUser(connection)} не имеет прав для physical бэкапа БД '{database}'. " +
                "Требуется членство в server-роли sysadmin, либо в db_owner или db_backupoperator целевой БД. " +
                $"Пример: USE [{database}]; ALTER ROLE db_backupoperator ADD MEMBER [{MssqlConnectionFactory.GrantMemberName(connection)}];");
        }
        catch (SqlException ex)
        {
            throw BuildValidationSqlException(ex, connection, database);
        }
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.DatabasePathSegment}_{timestamp}.bak";

        if (string.IsNullOrWhiteSpace(config.OutputPath))
        {
            throw new BackupUserFacingException(
                $"Для БД '{config.Database}' не задан OutputPath. " +
                "Для MSSQL physical backup укажите Databases[].OutputPath - каталог, доступный агенту и SQL Server.");
        }

        var outputPath = Path.GetFullPath(config.OutputPath);
        Directory.CreateDirectory(outputPath);

        var sqlFilePath = Path.Combine(outputPath, fileName);
        var agentFilePath = sqlFilePath;
        var dataSource = MssqlConnectionFactory.DescribeDataSource(connection);

        _logger.LogInformation(
            "Starting MSSQL physical backup. Database: '{Database}', DataSource: '{DataSource}', " +
            "SQL path: '{SqlPath}', Agent path: '{AgentPath}'",
            config.Database, dataSource, sqlFilePath, agentFilePath);

        var escapedDb = config.Database.Replace("]", "]]");
        var escapedPath = sqlFilePath.Replace("'", "''");
        var tsql = $"BACKUP DATABASE [{escapedDb}] TO DISK = N'{escapedPath}' WITH FORMAT, INIT, STATS = 10;";

        var connectionString = MssqlConnectionFactory.BuildMasterConnectionString(connection);

        var sw = Stopwatch.StartNew();

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(tsql, conn) { CommandTimeout = 0 };
        cmd.StatementCompleted += (_, e) =>
            _logger.LogDebug("MSSQL backup progress: {RecordsAffected} rows affected", e.RecordCount);

        try
        {
            await cmd.ExecuteNonQueryAsync(ct);

            ct.ThrowIfCancellationRequested();

            sw.Stop();

            if (!File.Exists(agentFilePath))
            {
                throw new BackupUserFacingException(
                    $"Файл бэкапа '{agentFilePath}' недоступен на хосте агента. " +
                    "Проверьте, что OutputPath указывает на каталог, доступный агенту и SQL Server.");
            }

            var sizeBytes = new FileInfo(agentFilePath).Length;

            ct.ThrowIfCancellationRequested();

            _logger.LogInformation(
                "MSSQL physical backup completed successfully. File: '{FilePath}', Size: {SizeBytes} bytes, Duration: {DurationMs} ms",
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
                "MSSQL physical backup cancelled for '{Database}', attempting cleanup of '{Path}'",
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

    private static Exception BuildSqlBackupException(
        SqlException ex,
        DatabaseConfig config,
        ConnectionConfig connection,
        string sqlFilePath)
    {
        if (HasError(ex, PermissionErrorNumbers))
        {
            return new BackupPermissionException(
                $"{MssqlConnectionFactory.DescribeUser(connection)} не имеет прав для MSSQL physical backup БД '{config.Database}'. " +
                "Требуются права BACKUP DATABASE или членство в роли db_backupoperator, db_owner либо sysadmin.",
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
                $"БД '{config.Database}' недоступна для MSSQL physical backup. Проверьте, что база online и файлы БД доступны SQL Server.",
                ex);
        }

        if (HasError(ex, 3201, 3202, 18204, 18210))
        {
            return new BackupUserFacingException(
                $"SQL Server не смог записать файл бэкапа по пути '{sqlFilePath}'. " +
                "Проверьте, что Databases[].OutputPath виден SQL Server и у service account SQL Server есть права на запись.",
                ex);
        }

        if (HasError(ex, 3013, 3041))
        {
            return new BackupUserFacingException(
                $"MSSQL не смог создать physical backup БД '{config.Database}'. Проверьте права, состояние БД и доступность Databases[].OutputPath для SQL Server.",
                ex);
        }

        return new BackupUserFacingException(
            $"MSSQL не смог создать physical backup БД '{config.Database}'. Подробности смотрите в логах агента.",
            ex);
    }

    private static Exception BuildValidationSqlException(
        SqlException ex,
        ConnectionConfig connection,
        string database)
    {
        if (HasError(ex, 18456) || HasError(ex, PermissionErrorNumbers))
        {
            return new BackupPermissionException(
                $"Не удалось проверить права MSSQL-пользователя подключения '{connection.Name}' для physical backup БД '{database}'. " +
                "Проверьте учётные данные подключения и права пользователя.",
                ex);
        }

        if (HasError(ex, 4060, 911))
        {
            return new BackupUserFacingException(
                $"БД '{database}' не найдена или недоступна для пользователя подключения '{connection.Name}'. Проверьте имя БД и права доступа.",
                ex);
        }

        if (HasError(ex, 924, 927, 942, 945, 952))
        {
            return new BackupUserFacingException(
                $"БД '{database}' недоступна для MSSQL physical backup. Проверьте, что база online и файлы БД доступны SQL Server.",
                ex);
        }

        return new BackupUserFacingException(
            $"MSSQL не смог проверить права для physical backup БД '{database}'. Подробности смотрите в логах агента.",
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

    private void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Could not delete partial backup file '{Path}'", path); }
    }
}
