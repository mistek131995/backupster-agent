using BackupsterAgent.Configuration;
using Microsoft.Data.SqlClient;

namespace BackupsterAgent.Services.Common.Resolvers;

internal static class MssqlConnectionFactory
{
    public static string BuildDatabaseConnectionString(ConnectionConfig connection, string database) =>
        BuildConnectionString(connection, database);

    public static string BuildMasterConnectionString(ConnectionConfig connection) =>
        BuildConnectionString(connection, "master");

    private static string BuildConnectionString(ConnectionConfig connection, string database) =>
        new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = database,
            UserID = connection.Username,
            Password = connection.Password,
            TrustServerCertificate = true,
            Encrypt = true,
        }.ConnectionString;
}
