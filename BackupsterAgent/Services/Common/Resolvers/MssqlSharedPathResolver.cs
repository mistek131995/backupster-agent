using BackupsterAgent.Configuration;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Services.Common.Resolvers;

public static class MssqlSharedPathResolver
{
    private static readonly HashSet<string> LocalHostAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        "localhost", "127.0.0.1", "::1", "0.0.0.0",
    };

    public static bool IsLocalHost(string host)
    {
        if (string.IsNullOrWhiteSpace(host)) return false;
        if (LocalHostAliases.Contains(host)) return true;
        return host.Equals(Environment.MachineName, StringComparison.OrdinalIgnoreCase);
    }

    public static string JoinSqlPath(string dir, string fileName)
    {
        var trimmed = dir.TrimEnd('/', '\\');
        var usesBackslash = trimmed.Contains('\\')
            || (trimmed.Length >= 2 && trimmed[1] == ':');
        var separator = usesBackslash ? "\\" : "/";
        return trimmed + separator + fileName;
    }

    public static async Task<string> GetSqlDirAsync(ConnectionConfig connection, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(connection.SharedBackupPath))
            return connection.SharedBackupPath!;

        return await QueryInstanceDefaultBackupPathAsync(connection, ct);
    }

    public static async Task<string> GetAgentDirAsync(ConnectionConfig connection, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(connection.AgentBackupPath))
            return connection.AgentBackupPath!;

        if (!string.IsNullOrWhiteSpace(connection.SharedBackupPath))
            return connection.SharedBackupPath!;

        if (!IsLocalHost(connection.Host))
            throw new InvalidOperationException(BuildRemoteNoSharedMessage(connection));

        return await QueryInstanceDefaultBackupPathAsync(connection, ct);
    }

    private static async Task<string> QueryInstanceDefaultBackupPathAsync(ConnectionConfig connection, CancellationToken ct)
    {
        const string sql = "SELECT CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS nvarchar(4000));";

        await using var conn = new SqlConnection(BuildMasterConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync(ct);

        if (result is null || result is DBNull || result is not string path || string.IsNullOrWhiteSpace(path))
        {
            throw new InvalidOperationException(
                $"SQL Server (подключение '{connection.Name}') не вернул InstanceDefaultBackupPath. " +
                "Задайте SharedBackupPath в ConnectionConfig — путь к каталогу .bak, видимый и агенту, и SQL Server.");
        }

        return path;
    }

    private static string BuildMasterConnectionString(ConnectionConfig connection) =>
        new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = "master",
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ToString();

    private static string BuildRemoteNoSharedMessage(ConnectionConfig connection) =>
        $"Remote MSSQL ('{connection.Host}', подключение '{connection.Name}') требует SharedBackupPath " +
        "в ConnectionConfig — путь к каталогу .bak, видимый и агенту, и SQL Server. Подробнее — в docs/mssql.md.";
}
