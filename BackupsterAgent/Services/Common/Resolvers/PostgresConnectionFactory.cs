using BackupsterAgent.Configuration;
using Npgsql;

namespace BackupsterAgent.Services.Common.Resolvers;

internal static class PostgresConnectionFactory
{
    public static string BuildDatabaseConnectionString(ConnectionConfig connection, string database) =>
        new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Port = connection.Port,
            Username = connection.Username,
            Password = connection.Password,
            Database = database,
            TcpKeepAlive = true,
            KeepAlive = 30,
        }.ToString();

    public static string BuildAdminConnectionString(ConnectionConfig connection) =>
        BuildDatabaseConnectionString(connection, "postgres");
}
