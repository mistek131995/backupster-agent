using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

namespace BackupsterAgent.Tests.Providers;

[TestFixture]
public sealed class PostgresRestoredReadinessConnectionResolverTests
{
    private string _pgData = null!;

    [SetUp]
    public void SetUp()
    {
        _pgData = Path.Combine(Path.GetTempPath(), "backupster-pg-readiness-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_pgData);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (Directory.Exists(_pgData))
                Directory.Delete(_pgData, recursive: true);
        }
        catch
        {
        }
    }

    [Test]
    public void Resolve_WhenRestoredConfigDoesNotSetPort_ReturnsSourceConnection()
    {
        var source = Connection(55432);

        var resolved = PostgresRestoredReadinessConnectionResolver.Resolve(source, _pgData);

        Assert.That(resolved, Is.SameAs(source));
    }

    [Test]
    public void Resolve_WhenPostgresqlConfSetsPort_UsesRestoredPort()
    {
        File.WriteAllText(Path.Combine(_pgData, "postgresql.conf"), "port = 35432 # restored port");
        var source = Connection(55432);

        var resolved = PostgresRestoredReadinessConnectionResolver.Resolve(source, _pgData);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.Not.SameAs(source));
            Assert.That(resolved.Port, Is.EqualTo(35432));
            Assert.That(resolved.Host, Is.EqualTo(source.Host));
            Assert.That(resolved.Username, Is.EqualTo(source.Username));
            Assert.That(resolved.Password, Is.EqualTo(source.Password));
            Assert.That(resolved.BinPath, Is.EqualTo(source.BinPath));
        });
    }

    [Test]
    public void TryReadConfiguredPort_WhenAutoConfSetsPort_OverridesPostgresqlConf()
    {
        File.WriteAllText(Path.Combine(_pgData, "postgresql.conf"), "port = 1111");
        File.WriteAllText(Path.Combine(_pgData, "postgresql.auto.conf"), "port = '2222'");

        var port = PostgresRestoredReadinessConnectionResolver.TryReadConfiguredPort(_pgData);

        Assert.That(port, Is.EqualTo(2222));
    }

    [Test]
    public void TryReadConfiguredPort_IgnoresCommentsAndQuotedCommentMarkers()
    {
        File.WriteAllText(
            Path.Combine(_pgData, "postgresql.conf"),
            string.Join(Environment.NewLine,
            [
                "# port = 1111",
                "application_name = 'value # not a comment'",
                "port = \"3333\" # active port",
            ]));

        var port = PostgresRestoredReadinessConnectionResolver.TryReadConfiguredPort(_pgData);

        Assert.That(port, Is.EqualTo(3333));
    }

    private static ConnectionConfig Connection(int port) => new()
    {
        Name = "pg-target",
        DatabaseType = DatabaseType.Postgres,
        ConnectionUri = "postgresql://unused",
        Host = "localhost",
        Port = port,
        Username = "postgres",
        Password = "pw",
        BinPath = "/usr/lib/postgresql/bin",
    };
}
