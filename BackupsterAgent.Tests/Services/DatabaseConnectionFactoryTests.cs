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

    [Test]
    public void MssqlDatabaseConnection_ConnectionUriUsesTargetDatabase()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=sql.internal,1444;Initial Catalog=source_db;Integrated Security=true;Encrypt=false;TrustServerCertificate=false;Application Name=bp-agent",
        };

        var connectionString = MssqlConnectionFactory.BuildDatabaseConnectionString(connection, "tenant_db");
        var parsed = new SqlConnectionStringBuilder(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.DataSource, Is.EqualTo("sql.internal,1444"));
            Assert.That(parsed.InitialCatalog, Is.EqualTo("tenant_db"));
            Assert.That(parsed.IntegratedSecurity, Is.True);
            Assert.That(parsed.UserID, Is.Empty);
            Assert.That(parsed.Password, Is.Empty);
            Assert.That(parsed.Encrypt.ToString(), Is.EqualTo("False"));
            Assert.That(parsed.TrustServerCertificate, Is.False);
            Assert.That(parsed.ApplicationName, Is.EqualTo("bp-agent"));
        });
    }

    [Test]
    public void MssqlDatabaseConnection_MixedConnectionUriAndLegacyFieldsRejected()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-mixed",
            ConnectionUri = "Server=sql.internal,1444;User ID=uri_user;Password=uri_secret",
            Host = "legacy-host",
            Port = 15433,
            Username = "legacy_user",
            Password = "legacy_secret",
        };

        Assert.Throws<InvalidOperationException>(() =>
            MssqlConnectionFactory.BuildDatabaseConnectionString(connection, "tenant_db"));
    }

    [Test]
    public void MssqlMasterConnection_ConnectionUriUsesMasterDatabase()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=sql.internal,1444;Database=source_db;User ID=uri_user;Password=uri_secret;Encrypt=true;TrustServerCertificate=true",
        };

        var connectionString = MssqlConnectionFactory.BuildMasterConnectionString(connection);
        var parsed = new SqlConnectionStringBuilder(connectionString);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.DataSource, Is.EqualTo("sql.internal,1444"));
            Assert.That(parsed.InitialCatalog, Is.EqualTo("master"));
            Assert.That(parsed.UserID, Is.EqualTo("uri_user"));
            Assert.That(parsed.Password, Is.EqualTo("uri_secret"));
        });
    }

    [Test]
    public void MssqlBuildTopologyEndpoint_ConnectionUriParsesTcpHostPort()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=tcp:sql.internal,1444;User ID=uri_user;Password=uri_secret;Encrypt=true",
        };

        var endpoint = MssqlConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.Multiple(() =>
        {
            Assert.That(endpoint, Is.Not.Null);
            Assert.That(endpoint!.Host, Is.EqualTo("sql.internal"));
            Assert.That(endpoint.Port, Is.EqualTo(1444));
        });
    }

    [Test]
    public void MssqlBuildTopologyEndpoint_ConnectionUriWithoutPortUsesDefaultPort()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=sql.internal;Integrated Security=true",
        };

        var endpoint = MssqlConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.Multiple(() =>
        {
            Assert.That(endpoint, Is.Not.Null);
            Assert.That(endpoint!.Host, Is.EqualTo("sql.internal"));
            Assert.That(endpoint.Port, Is.EqualTo(1433));
        });
    }

    [Test]
    public void MssqlBuildTopologyEndpoint_NamedInstanceReturnsNull()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=sql.internal\\reporting;User ID=uri_user;Password=uri_secret",
        };

        var endpoint = MssqlConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.That(endpoint, Is.Null);
    }

    [Test]
    public void MssqlBuildTopologyEndpoint_AdminChannelReturnsNull()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = "Server=admin:sql.internal,1434;User ID=uri_user;Password=uri_secret",
        };

        var endpoint = MssqlConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.That(endpoint, Is.Null);
    }

    [TestCase("Server=.;Integrated Security=true")]
    [TestCase("Server=(local);Integrated Security=true")]
    public void MssqlBuildTopologyEndpoint_LocalAliasNormalizedToLocalhost(string connectionUri)
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-uri",
            ConnectionUri = connectionUri,
        };

        var endpoint = MssqlConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.Multiple(() =>
        {
            Assert.That(endpoint, Is.Not.Null);
            Assert.That(endpoint!.Host, Is.EqualTo("localhost"));
            Assert.That(endpoint.Port, Is.EqualTo(1433));
        });
    }

    [Test]
    public void MssqlBuildTopologyEndpoint_MixedConnectionUriAndLegacyFieldsRejected()
    {
        var connection = new ConnectionConfig
        {
            Name = "mssql-mixed",
            ConnectionUri = "Server=sql.internal,1444;User ID=uri_user;Password=uri_secret",
            Host = "legacy-host",
        };

        Assert.Throws<InvalidOperationException>(() =>
            MssqlConnectionFactory.BuildTopologyEndpoint(connection));
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
