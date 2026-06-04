using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common.Resolvers;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class MongoConnectionFactoryTests
{
    [Test]
    public void BuildToolUri_LegacyConnection_UsesHostPortAndAdminAuthSource()
    {
        var connection = new ConnectionConfig
        {
            Name = "mongo",
            DatabaseType = DatabaseType.MongoDb,
            Host = "mongo.internal",
            Port = 27017,
            Username = "backup user",
            Password = "p@ss/w:rd",
        };

        var uri = MongoConnectionFactory.BuildToolUri(connection);

        Assert.That(
            uri,
            Is.EqualTo("mongodb://backup%20user:p%40ss%2Fw%3Ard@mongo.internal:27017/?authSource=admin"));
    }

    [Test]
    public void BuildToolUri_ConnectionUri_ReturnsConfiguredUri()
    {
        var connection = new ConnectionConfig
        {
            Name = "atlas",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "  mongodb+srv://user:pass@cluster.example.net/?tls=true&authSource=admin  ",
        };

        var uri = MongoConnectionFactory.BuildToolUri(connection);

        Assert.That(
            uri,
            Is.EqualTo("mongodb+srv://user:pass@cluster.example.net/?tls=true&authSource=admin"));
    }

    [Test]
    public void BuildToolUri_InvalidScheme_ThrowsWithoutLeakingUri()
    {
        var connection = new ConnectionConfig
        {
            Name = "bad",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "postgres://user:secret@host/db",
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MongoConnectionFactory.BuildToolUri(connection));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("mongodb://"));
            Assert.That(ex.Message, Does.Not.Contain("secret"));
            Assert.That(ex.Message, Does.Not.Contain("postgres://"));
        });
    }

    [Test]
    public void BuildToolUri_ConnectionUriMixedWithHost_Throws()
    {
        var connection = new ConnectionConfig
        {
            Name = "mixed",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "mongodb://user:secret@host",
            Host = "other-host",
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MongoConnectionFactory.BuildToolUri(connection));

        Assert.That(ex!.Message, Does.Contain("либо ConnectionUri"));
    }

    [Test]
    public void BuildTopologyEndpoint_ConnectionUri_StripsCredentialsAndQuery()
    {
        var connection = new ConnectionConfig
        {
            Name = "atlas",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "mongodb://user:secret@cluster.example.net:27019/?tls=true&tlsCAFile=/etc/mongo-ca.pem",
        };

        var endpoint = MongoConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.Multiple(() =>
        {
            Assert.That(endpoint, Is.Not.Null);
            Assert.That(endpoint!.Host, Is.EqualTo("cluster.example.net"));
            Assert.That(endpoint.Port, Is.EqualTo(27019));
        });
    }

    [Test]
    public void BuildTopologyEndpoint_SrvConnectionUri_ReturnsSrvHost()
    {
        var connection = new ConnectionConfig
        {
            Name = "atlas",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "mongodb+srv://user:secret@cluster.example.net/?tls=true",
        };

        var endpoint = MongoConnectionFactory.BuildTopologyEndpoint(connection);

        Assert.Multiple(() =>
        {
            Assert.That(endpoint, Is.Not.Null);
            Assert.That(endpoint!.Host, Is.EqualTo("cluster.example.net"));
            Assert.That(endpoint.Port, Is.InRange(1, 65535));
        });
    }

    [Test]
    public void BuildTopologyEndpoint_ConnectionUriWithZeroPort_ThrowsWithoutLeakingUri()
    {
        var connection = new ConnectionConfig
        {
            Name = "atlas",
            DatabaseType = DatabaseType.MongoDb,
            ConnectionUri = "mongodb://user:secret@cluster.example.net:0/?tls=true&tlsCAFile=/etc/ca.pem",
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MongoConnectionFactory.BuildTopologyEndpoint(connection));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("ConnectionUri"));
            Assert.That(ex.Message, Does.Not.Contain("secret"));
            Assert.That(ex.Message, Does.Not.Contain("tlsCAFile"));
            Assert.That(ex.Message, Does.Not.Contain("/etc/ca.pem"));
        });
    }

    [Test]
    public void Redact_RemovesCredentialsAndQueryFromMongoUris()
    {
        const string input =
            "error connecting to mongodb+srv://user:secret@cluster.example.net/?tls=true&tlsCAFile=/etc/ca.pem";

        var redacted = MongoConnectionFactory.Redact(input);

        Assert.Multiple(() =>
        {
            Assert.That(redacted, Does.Contain("mongodb+srv://<redacted>@cluster.example.net/"));
            Assert.That(redacted, Does.Not.Contain("user"));
            Assert.That(redacted, Does.Not.Contain("secret"));
            Assert.That(redacted, Does.Not.Contain("tlsCAFile"));
            Assert.That(redacted, Does.Not.Contain("/etc/ca.pem"));
        });
    }
}
