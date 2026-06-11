using BackupsterAgent.Providers.Backup;
using Microsoft.SqlServer.Dac;

namespace BackupsterAgent.Tests.Providers;

public sealed class MssqlLogicalPreflightResultTests
{
    [Test]
    public void BuildBlockingUserMessage_IncludesBlockingFindingsAndSkipsWarnings()
    {
        var result = new MssqlLogicalPreflightResult(
        [
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.CrossDatabaseReference,
                "[dbo].[v_orders] -> [master].[dbo].[objects]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.FileStream,
                "[dbo].[documents].[content]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.OrphanedUser,
                "[legacy_user]"),
        ]);

        var message = result.BuildBlockingUserMessage("sales");

        Assert.Multiple(() =>
        {
            Assert.That(result.HasBlockingFindings, Is.True);
            Assert.That(result.WarningFindings, Has.Count.EqualTo(1));
            Assert.That(message, Does.Contain("sales"));
            Assert.That(message, Does.Contain("[master].[dbo].[objects]"));
            Assert.That(message, Does.Contain("[dbo].[documents].[content]"));
            Assert.That(message, Does.Not.Contain("[legacy_user]"));
        });
    }

    [Test]
    public void WarningOnlyFindings_DoNotBlockBackup()
    {
        var result = new MssqlLogicalPreflightResult(
        [
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.OrphanedUser,
                "[legacy_user]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.ClrUnsafeOrExternal,
                "[UnsafeAssembly] (UNSAFE_ACCESS)"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.TdeEnabled,
                "database encryption is enabled"),
        ]);

        var warning = result.BuildWarningLogMessage("sales");

        Assert.Multiple(() =>
        {
            Assert.That(result.HasBlockingFindings, Is.False);
            Assert.That(result.HasWarningFindings, Is.True);
            Assert.That(warning, Does.Contain("legacy_user"));
            Assert.That(warning, Does.Contain("UnsafeAssembly"));
            Assert.That(warning, Does.Contain("TDE"));
            Assert.Throws<InvalidOperationException>(() => result.BuildBlockingUserMessage("sales"));
        });
    }

    [Test]
    public void BuildBlockingUserMessage_CapsRepeatedDetailsPerFindingCode()
    {
        var result = new MssqlLogicalPreflightResult(
        [
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.CrossDatabaseReference,
                "[dbo].[v1] -> [db1].[dbo].[t]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.CrossDatabaseReference,
                "[dbo].[v2] -> [db2].[dbo].[t]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.CrossDatabaseReference,
                "[dbo].[v3] -> [db3].[dbo].[t]"),
            MssqlLogicalPreflightResult.CreateFinding(
                MssqlLogicalPreflightResult.FindingCode.CrossDatabaseReference,
                "[dbo].[v4] -> [db4].[dbo].[t]"),
        ]);

        var message = result.BuildBlockingUserMessage("sales");

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("[db1].[dbo].[t]"));
            Assert.That(message, Does.Contain("[db2].[dbo].[t]"));
            Assert.That(message, Does.Contain("[db3].[dbo].[t]"));
            Assert.That(message, Does.Not.Contain("[db4].[dbo].[t]"));
            Assert.That(message, Does.Contain("+1"));
        });
    }

    [Test]
    public void ExtractErrorMessages_PrefersExceptionMessagesOverEventBuffer()
    {
        var exceptionMessages = new[]
        {
            DacMessage(DacMessageType.Error, 72014, "exception reason"),
            DacMessage(DacMessageType.Error, 72014, "exception reason"),
        };
        var fallbackMessages = new[]
        {
            DacMessage(DacMessageType.Error, 72045, "fallback reason"),
        };

        var result = MssqlDacFxErrorFormatter.ExtractErrorMessages(exceptionMessages, fallbackMessages);

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(result[0], Does.Contain("exception reason"));
            Assert.That(result[0], Does.Not.Contain("fallback reason"));
        });
    }

    [Test]
    public void ExtractErrorMessages_FallsBackToDistinctEventBufferMessages()
    {
        var fallbackMessages = new[]
        {
            DacMessage(DacMessageType.Error, 1, "first"),
            DacMessage(DacMessageType.Error, 1, "first"),
            DacMessage(DacMessageType.Error, 2, "second"),
            DacMessage(DacMessageType.Error, 3, "third"),
            DacMessage(DacMessageType.Error, 4, "fourth"),
        };

        var result = MssqlDacFxErrorFormatter.ExtractErrorMessages([], fallbackMessages);

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(3));
            Assert.That(result[0], Does.Contain("first"));
            Assert.That(result[1], Does.Contain("second"));
            Assert.That(result[2], Does.Contain("third"));
            Assert.That(result.Any(message => message.Contains("fourth", StringComparison.Ordinal)), Is.False);
        });
    }

    [Test]
    public void BuildExportFailureMessage_UsesExceptionMessageWhenNoDacMessagesExist()
    {
        var message = MssqlDacFxErrorFormatter.BuildExportFailureMessage(
            "sales",
            exceptionMessages: [],
            fallbackMessages: [],
            exceptionMessage: "outer DacFx reason");

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("sales"));
            Assert.That(message, Does.Contain("outer DacFx reason"));
        });
    }

    private static DacMessage DacMessage(DacMessageType type, int number, string message) =>
        new(type, number, message, "DacFx", string.Empty);
}
