using BackupsterAgent.Configuration;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Services.Common.Resolvers;

internal sealed record MssqlTopologyEndpoint(string Host, int Port);

internal static class MssqlConnectionFactory
{
    private const int DefaultPort = 1433;

    public static bool HasConnectionUri(ConnectionConfig connection) =>
        !string.IsNullOrWhiteSpace(connection.ConnectionUri);

    public static string BuildDatabaseConnectionString(ConnectionConfig connection, string database) =>
        BuildConnectionString(connection, database);

    public static string BuildMasterConnectionString(ConnectionConfig connection) =>
        BuildConnectionString(connection, "master");

    public static MssqlTopologyEndpoint? BuildTopologyEndpoint(ConnectionConfig connection)
    {
        if (!HasConnectionUri(connection))
        {
            if (string.IsNullOrWhiteSpace(connection.Host))
                return null;

            return new MssqlTopologyEndpoint(connection.Host, connection.Port);
        }

        var builder = BuildConnectionUriBuilder(connection);
        return TryParseDataSource(builder.DataSource, out var host, out var port)
            ? new MssqlTopologyEndpoint(host, port)
            : null;
    }

    public static string DescribeUser(ConnectionConfig connection) =>
        HasConnectionUri(connection)
            ? $"Пользователь из ConnectionUri подключения '{connection.Name}'"
            : $"Пользователь '{connection.Username}' подключения '{connection.Name}'";

    public static string GrantMemberName(ConnectionConfig connection) =>
        string.IsNullOrWhiteSpace(connection.Username) ? "<пользователь>" : connection.Username;

    public static string DescribeDataSource(ConnectionConfig connection)
    {
        if (HasConnectionUri(connection))
        {
            var builder = BuildConnectionUriBuilder(connection);
            return string.IsNullOrWhiteSpace(builder.DataSource)
                ? "(empty data source)"
                : builder.DataSource;
        }

        return $"{connection.Host}:{connection.Port}";
    }

    private static string BuildConnectionString(ConnectionConfig connection, string database)
    {
        if (HasConnectionUri(connection))
        {
            var builder = BuildConnectionUriBuilder(connection);
            builder.InitialCatalog = database;
            return builder.ConnectionString;
        }

        return new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = database,
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ConnectionString;
    }

    private static SqlConnectionStringBuilder BuildConnectionUriBuilder(ConnectionConfig connection)
    {
        if (!string.IsNullOrWhiteSpace(connection.Host) ||
            !string.IsNullOrWhiteSpace(connection.Username) ||
            !string.IsNullOrWhiteSpace(connection.Password))
        {
            throw new InvalidOperationException(
                $"Для MSSQL-подключения '{connection.Name}' укажите либо ConnectionUri, либо Host/Port/Username/Password.");
        }

        try
        {
            return new SqlConnectionStringBuilder(connection.ConnectionUri!.Trim());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"ConnectionUri для MSSQL-подключения '{connection.Name}' задан некорректно.", ex);
        }
    }

    private static bool TryParseDataSource(string? dataSource, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        var raw = dataSource?.Trim();
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        if (raw.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
            raw = raw[4..].Trim();
        else if (raw.StartsWith("np:", StringComparison.OrdinalIgnoreCase) ||
                 raw.StartsWith("lpc:", StringComparison.OrdinalIgnoreCase) ||
                 raw.StartsWith("admin:", StringComparison.OrdinalIgnoreCase))
            return false;

        if (raw.StartsWith("(localdb)", StringComparison.OrdinalIgnoreCase) ||
            raw.Contains('\\'))
            return false;

        var commaIndex = raw.LastIndexOf(',');
        if (commaIndex >= 0)
        {
            host = raw[..commaIndex].Trim();
            var portText = raw[(commaIndex + 1)..].Trim();
            if (!int.TryParse(portText, out port))
                return false;
        }
        else
        {
            host = raw;
            port = DefaultPort;
        }

        if (host.Length >= 2 && host[0] == '[' && host[^1] == ']')
            host = host[1..^1];

        if (host == "." || host.Equals("(local)", StringComparison.OrdinalIgnoreCase))
            host = "localhost";

        return !string.IsNullOrWhiteSpace(host) && port is > 0 and <= 65535;
    }
}
