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
}
