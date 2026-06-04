using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class DatabaseConnectionFactoryTests
{
    [Test]
    public void PostgresDatabaseConnection_UsesDatabaseAndKeepAliveSettings()
    {
        var connection = BuildConnection(port: 5433);

        var connectionString = PostgresConnectionFactory.BuildDatabaseConnectionString(connection, "tenant_db");
        var parsed = new NpgsqlConnectionStringBuilder(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Host, Is.EqualTo("db.internal"));
            Assert.That(parsed.Port, Is.EqualTo(5433));
            Assert.That(parsed.Username, Is.EqualTo("backup_user"));
            Assert.That(parsed.Password, Is.EqualTo("secret"));
            Assert.That(parsed.Database, Is.EqualTo("tenant_db"));
            Assert.That(parsed.TcpKeepAlive, Is.True);
            Assert.That(parsed.KeepAlive, Is.EqualTo(30));
        });
    }

    [Test]
    public void PostgresAdminConnection_UsesPostgresDatabase()
    {
        var connection = BuildConnection(port: 5432);

        var connectionString = PostgresConnectionFactory.BuildAdminConnectionString(connection);
        var parsed = new NpgsqlConnectionStringBuilder(connectionString);

        Assert.That(parsed.Database, Is.EqualTo("postgres"));
    }

    [Test]
    public void MysqlServerConnection_DoesNotSelectDatabase()
    {
        var connection = BuildConnection(port: 3307);

        var connectionString = MysqlConnectionFactory.BuildServerConnectionString(connection);
        var parsed = new MySqlConnectionStringBuilder(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Server, Is.EqualTo("db.internal"));
            Assert.That(parsed.Port, Is.EqualTo(3307));
            Assert.That(parsed.UserID, Is.EqualTo("backup_user"));
            Assert.That(parsed.Password, Is.EqualTo("secret"));
            Assert.That(parsed.Database, Is.Empty);
        });
    }

    [Test]
    public void MysqlAdminConnection_UsesInformationSchema()
    {
        var connection = BuildConnection(port: 3306);

        var connectionString = MysqlConnectionFactory.BuildAdminConnectionString(connection);
        var parsed = new MySqlConnectionStringBuilder(connectionString);

        Assert.That(parsed.Database, Is.EqualTo("information_schema"));
    }

    [Test]
    public void MssqlDatabaseConnection_UsesTargetDatabaseAndEncryptionSettings()
    {
        var connection = BuildConnection(port: 1435);

        var connectionString = MssqlConnectionFactory.BuildDatabaseConnectionString(connection, "tenant_db");
        var parsed = new SqlConnectionStringBuilder(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.DataSource, Is.EqualTo("db.internal,1435"));
            Assert.That(parsed.InitialCatalog, Is.EqualTo("tenant_db"));
            Assert.That(parsed.UserID, Is.EqualTo("backup_user"));
            Assert.That(parsed.Password, Is.EqualTo("secret"));
            Assert.That(parsed.Encrypt.ToString(), Is.EqualTo("True"));
            Assert.That(parsed.TrustServerCertificate, Is.True);
        });
    }

    [Test]
    public void MssqlMasterConnection_UsesMasterDatabase()
    {
        var connection = BuildConnection(port: 1433);

        var connectionString = MssqlConnectionFactory.BuildMasterConnectionString(connection);
        var parsed = new SqlConnectionStringBuilder(connectionString);

        Assert.That(parsed.InitialCatalog, Is.EqualTo("master"));
    }

    private static ConnectionConfig BuildConnection(int port) =>
        new()
        {
            Name = "main",
            Host = "db.internal",
            Port = port,
            Username = "backup_user",
            Password = "secret",
        };
}
