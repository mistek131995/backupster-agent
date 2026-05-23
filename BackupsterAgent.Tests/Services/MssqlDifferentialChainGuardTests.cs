using BackupsterAgent.Providers.Backup;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class MssqlDifferentialChainGuardTests
{
    [Test]
    public void Classify_ParentMissing_ReturnsParentMissing()
    {
        var result = MssqlDifferentialChainGuard.Classify(parentCount: 0, foreignCount: 0);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ParentMissing));
    }

    [Test]
    public void Classify_ParentMissingButForeignCountFromQuirkyDb_StillReturnsParentMissing()
    {
        var result = MssqlDifferentialChainGuard.Classify(parentCount: 0, foreignCount: 42);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ParentMissing),
            "ParentMissing should win over foreign count because the chain anchor itself is gone");
    }

    [Test]
    public void Classify_ParentPresentNoForeign_ReturnsOk()
    {
        var result = MssqlDifferentialChainGuard.Classify(parentCount: 1, foreignCount: 0);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.Ok));
    }

    [Test]
    public void Classify_ParentPresentForeignAfter_ReturnsForeignFullDetected()
    {
        var result = MssqlDifferentialChainGuard.Classify(parentCount: 1, foreignCount: 1);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ForeignFullDetected));
    }
}
