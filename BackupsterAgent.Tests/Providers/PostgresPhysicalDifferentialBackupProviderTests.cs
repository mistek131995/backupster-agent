using BackupsterAgent.Providers.Backup;

namespace BackupsterAgent.Tests.Providers;

public sealed class PostgresPhysicalDifferentialBackupProviderTests
{
    [TestCase("ERROR: WAL summaries are required on timeline 1 from 0/20000D8 to 0/4000028", true)]
    [TestCase("ERROR: WAL summarizer has not caught up yet", true)]
    [TestCase("ERROR: could not connect to server", false)]
    public void IsWalSummaryFailure_ClassifiesPgBasebackupStderr(string stderr, bool expected)
    {
        Assert.That(PostgresPhysicalDifferentialBackupProvider.IsWalSummaryFailure(stderr), Is.EqualTo(expected));
    }

    [Test]
    public void ReadRequiredWalEndLsn_ReturnsLargestManifestEndLsn()
    {
        var manifestPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"{Guid.NewGuid():N}.backup_manifest");
        File.WriteAllText(manifestPath, """
            {
              "PostgreSQL-Backup-Manifest-Version": 2,
              "WAL-Ranges": [
                { "Timeline": 1, "Start-LSN": "1/7F000028", "End-LSN": "1/81000158" },
                { "Timeline": 1, "Start-LSN": "1/81000158", "End-LSN": "1/82000000" }
              ]
            }
            """);

        try
        {
            var required = PostgresPhysicalDifferentialBackupProvider.ReadRequiredWalPosition(manifestPath);
            Assert.That(required.Timeline, Is.EqualTo(1));
            Assert.That(required.EndLsn, Is.EqualTo("1/82000000"));
        }
        finally
        {
            File.Delete(manifestPath);
        }
    }

    [TestCase("1/7F000028", "1/81000158", -1)]
    [TestCase("1/81000158", "1/81000158", 0)]
    [TestCase("1/82000000", "1/81000158", 1)]
    public void ComparePgLsn_OrdersHexLsnValues(string left, string right, int expectedSign)
    {
        var actual = Math.Sign(PostgresPhysicalDifferentialBackupProvider.ComparePgLsn(left, right));
        Assert.That(actual, Is.EqualTo(expectedSign));
    }

    [Test]
    public void ParseWalFileTimeline_ReadsTimelinePrefix()
    {
        Assert.That(PostgresPhysicalDifferentialBackupProvider.ParseWalFileTimeline("00000002000000010000007F"),
            Is.EqualTo(2));
    }
}
