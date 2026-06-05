using System.Net.Sockets;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;
using MySqlConnector;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public enum MysqlConnectionProbeResult
{
    Reachable,
    ServerGone,
    TransientError,
}

public sealed class MysqlServerProbe : IMysqlServerProbe
{
    private readonly ILogger<MysqlServerProbe> _logger;

    public MysqlServerProbe(ILogger<MysqlServerProbe> logger)
    {
        _logger = logger;
    }

    public async Task<string> QueryDataDirectoryAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(MysqlConnectionFactory.BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand("SELECT @@datadir;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);
        var datadir = result as string ?? string.Empty;

        if (string.IsNullOrWhiteSpace(datadir))
            throw new RestorePermissionException(
                $"Не удалось получить datadir из MySQL-сервера подключения '{connection.Name}'.");

        return datadir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
    }

    public async Task EnsureShutdownPrivilegeAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(MysqlConnectionFactory.BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand(
            "SELECT COUNT(*) FROM information_schema.user_privileges " +
            "WHERE GRANTEE = CONCAT('''', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), '''@''', " +
            "SUBSTRING_INDEX(CURRENT_USER(), '@', -1), '''') " +
            "AND privilege_type IN ('SHUTDOWN', 'SUPER')", conn);

        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

        if (count == 0)
            throw new RestorePermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' " +
                $"не имеет привилегии SHUTDOWN, необходимой для физического восстановления MySQL. " +
                $"Выдайте привилегию: GRANT SHUTDOWN ON *.* TO '{connection.Username}'@'%'; FLUSH PRIVILEGES;");
    }

    public async Task<int?> GetMysqlPidAsync(ConnectionConfig connection, CancellationToken ct)
    {
        try
        {
            await using var conn = new MySqlConnection(MysqlConnectionFactory.BuildServerConnectionString(connection));
            await conn.OpenAsync(ct);

            await using var cmd = new MySqlCommand("SELECT @@pid_file;", conn);
            var pidFile = await cmd.ExecuteScalarAsync(ct) as string;

            if (string.IsNullOrWhiteSpace(pidFile) || !File.Exists(pidFile))
                return null;

            var pidContent = (await File.ReadAllTextAsync(pidFile, ct)).Trim();
            return int.TryParse(pidContent, out var pid) ? pid : null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to resolve MySQL PID");
            return null;
        }
    }

    public async Task IssueShutdownAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(MysqlConnectionFactory.BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand("SHUTDOWN;", conn);
        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.UnableToConnectToHost
                                        || ex.Number == 0
                                        || ex.Message.Contains("connection was lost", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation("MySQL SHUTDOWN issued — connection closed as expected");
        }
    }

    public async Task<MysqlConnectionProbeResult> ProbeConnectionAsync(ConnectionConfig connection, CancellationToken ct)
    {
        try
        {
            await using var conn = new MySqlConnection(MysqlConnectionFactory.BuildServerConnectionString(connection));
            await conn.OpenAsync(ct);
            return MysqlConnectionProbeResult.Reachable;
        }
        catch (Exception ex) when (IsServerGoneException(ex))
        {
            return MysqlConnectionProbeResult.ServerGone;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Transient error while waiting for MySQL to stop — retrying");
            return MysqlConnectionProbeResult.TransientError;
        }
    }

    private static bool IsServerGoneException(Exception ex)
    {
        if (ex is SocketException { SocketErrorCode: SocketError.ConnectionRefused })
            return true;

        if (ex is MySqlException mysql)
        {
            if (mysql.ErrorCode == MySqlErrorCode.UnableToConnectToHost)
                return true;
            if (mysql.InnerException is SocketException { SocketErrorCode: SocketError.ConnectionRefused })
                return true;
        }

        return false;
    }

}
