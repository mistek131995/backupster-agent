using BackupsterAgent.Providers.Backup;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class MssqlDifferentialChainGuardTests
{
    [Test]
    public void Classify_ParentMissing_ReturnsParentMissing()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 0,
            parentBackupSetUuidPresent: false,
            foreignCount: 0,
            baseIsAmbiguous: true,
            baseMatchesParent: false);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ParentMissing));
    }

    [Test]
    public void Classify_ParentMissingButForeignCountFromQuirkyDb_StillReturnsParentMissing()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 0,
            parentBackupSetUuidPresent: false,
            foreignCount: 42,
            baseIsAmbiguous: false,
            baseMatchesParent: true);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ParentMissing),
            "ParentMissing should win over foreign count because the chain anchor itself is gone");
    }

    [Test]
    public void Classify_ParentPresentButUuidMissing_ReturnsParentMissing()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 1,
            parentBackupSetUuidPresent: false,
            foreignCount: 0,
            baseIsAmbiguous: false,
            baseMatchesParent: true);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ParentMissing));
    }

    [Test]
    public void Classify_ParentPresentButBaseAmbiguous_ReturnsBaseUnknownOrAmbiguous()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 1,
            parentBackupSetUuidPresent: true,
            foreignCount: 1,
            baseIsAmbiguous: true,
            baseMatchesParent: false);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.BaseUnknownOrAmbiguous),
            "Ambiguous current base should win before divergence and foreign-count checks");
    }

    [Test]
    public void Classify_ParentPresentButBaseDiverged_ReturnsBaseDiverged()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 1,
            parentBackupSetUuidPresent: true,
            foreignCount: 1,
            baseIsAmbiguous: false,
            baseMatchesParent: false);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.BaseDiverged),
            "Diverged current base should win before foreign-count checks");
    }

    [Test]
    public void Classify_ParentPresentNoForeignAndBaseMatches_ReturnsOk()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 1,
            parentBackupSetUuidPresent: true,
            foreignCount: 0,
            baseIsAmbiguous: false,
            baseMatchesParent: true);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.Ok));
    }

    [Test]
    public void Classify_ParentPresentForeignAfterAndBaseMatches_ReturnsForeignFullDetected()
    {
        var result = MssqlDifferentialChainGuard.Classify(
            parentCount: 1,
            parentBackupSetUuidPresent: true,
            foreignCount: 1,
            baseIsAmbiguous: false,
            baseMatchesParent: true);

        Assert.That(result, Is.EqualTo(MssqlDifferentialChainCheck.ForeignFullDetected));
    }
}
