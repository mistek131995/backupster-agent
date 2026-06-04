using BackupsterAgent.Configuration;
using MySqlConnector;

namespace BackupsterAgent.Services.Common.Resolvers;

internal static class MysqlConnectionFactory
{
    public static string BuildServerConnectionString(ConnectionConfig connection) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
        }.ToString();

    public static string BuildAdminConnectionString(ConnectionConfig connection) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
            Database = "information_schema",
        }.ToString();
}
