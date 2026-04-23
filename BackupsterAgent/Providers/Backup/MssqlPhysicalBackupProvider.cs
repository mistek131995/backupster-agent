using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Providers.Backup;

public sealed class MssqlPhysicalBackupProvider : IBackupProvider
{
    private readonly ILogger<MssqlPhysicalBackupProvider> _logger;

    public MssqlPhysicalBackupProvider(ILogger<MssqlPhysicalBackupProvider> logger)
    {
        _logger = logger;
    }

    public async Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var fileName = $"{config.Database}_{timestamp}.bak";

        var sqlDir = MssqlSharedPathResolver.GetSqlDir(connection, config.OutputPath);
        var agentDir = MssqlSharedPathResolver.GetAgentDir(connection, config.OutputPath);

        Directory.CreateDirectory(agentDir);

        var sqlFilePath = MssqlSharedPathResolver.JoinSqlPath(sqlDir, fileName);
        var agentFilePath = Path.Combine(agentDir, fileName);

        _logger.LogInformation(
            "Starting MSSQL physical backup. Database: '{Database}', Host: '{Host}:{Port}', " +
            "SQL path: '{SqlPath}', Agent path: '{AgentPath}'",
            config.Database, connection.Host, connection.Port, sqlFilePath, agentFilePath);

        var escapedDb = config.Database.Replace("]", "]]");
        var escapedPath = sqlFilePath.Replace("'", "''");
        var tsql = $"BACKUP DATABASE [{escapedDb}] TO DISK = N'{escapedPath}' WITH FORMAT, INIT, STATS = 10;";

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
            _logger.LogDebug("MSSQL backup progress: {RecordsAffected} rows affected", e.RecordCount);

        await cmd.ExecuteNonQueryAsync(ct);

        sw.Stop();

        if (!File.Exists(agentFilePath))
        {
            throw new InvalidOperationException(
                $"Backup file '{agentFilePath}' is not accessible from the agent host. " +
                "Проверьте, что SharedBackupPath и AgentBackupPath указывают на один и тот же каталог.");
        }

        var sizeBytes = new FileInfo(agentFilePath).Length;

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
}
