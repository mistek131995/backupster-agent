using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Workers.Handlers;

namespace BackupsterAgent.Tests.Workers;

[TestFixture]
public sealed class RestoreTaskHandlerTests
{
    [Test]
    public void CombineResults_DbSuccessFilesSuccess_OverallSuccess()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Success(),
            FileRestoreResult.Success(3));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Success));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Success));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Success));
            Assert.That(patch.ErrorMessage, Is.Null);
            Assert.That(patch.Restore!.FilesRestoredCount, Is.EqualTo(3));
            Assert.That(patch.Restore!.FilesFailedCount, Is.Null);
        });
    }

    [Test]
    public void CombineResults_DbSuccessFilesSkipped_OverallSuccess()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Success(),
            FileRestoreResult.Skipped());

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Success));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Success));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Skipped));
            Assert.That(patch.ErrorMessage, Is.Null);
            Assert.That(patch.Restore!.FilesRestoredCount, Is.Null);
            Assert.That(patch.Restore!.FilesFailedCount, Is.Null);
        });
    }

    [Test]
    public void CombineResults_DbSuccessFilesPartial_OverallPartialWithFilesError()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Success(),
            FileRestoreResult.Partial(restored: 5, failed: 2, errorMessage: "f-err"));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Partial));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Success));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Partial));
            Assert.That(patch.ErrorMessage, Is.EqualTo("f-err"));
            Assert.That(patch.Restore!.FilesRestoredCount, Is.EqualTo(5));
            Assert.That(patch.Restore!.FilesFailedCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void CombineResults_DbSuccessFilesFailed_OverallIsPartialBecauseDbRestored()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Success(),
            FileRestoreResult.Failed("f-err"));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Partial));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Success));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Failed));
            Assert.That(patch.ErrorMessage, Is.EqualTo("f-err"));
        });
    }

    [Test]
    public void CombineResults_DbFailedFilesSuccess_OverallFailed()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Failed("db-err"),
            FileRestoreResult.Success(4));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Failed));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Failed));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Success));
            Assert.That(patch.ErrorMessage, Is.EqualTo("db-err"));
            Assert.That(patch.Restore!.FilesRestoredCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void CombineResults_DbFailedFilesFailed_OverallFailedBothMessagesJoined()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Failed("db-err"),
            FileRestoreResult.Failed("f-err"));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Failed));
            Assert.That(patch.Restore!.DatabaseStatus, Is.EqualTo(RestoreDatabaseStatus.Failed));
            Assert.That(patch.Restore!.FilesStatus, Is.EqualTo(RestoreFilesStatus.Failed));
            Assert.That(patch.ErrorMessage, Is.EqualTo("db-err\n\nf-err"));
        });
    }

    [Test]
    public void CombineResults_ZeroRestoredCount_EmittedAsNullNotZero()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Success(),
            FileRestoreResult.Success(0));

        Assert.Multiple(() =>
        {
            Assert.That(patch.Restore!.FilesRestoredCount, Is.Null);
            Assert.That(patch.Restore!.FilesFailedCount, Is.Null);
        });
    }

    [Test]
    public void CombineResults_OnlyDbError_UsedAsErrorMessage()
    {
        var patch = RestoreTaskHandler.CombineResults(
            DatabaseRestoreResult.Failed("only-db"),
            FileRestoreResult.Success(1));

        Assert.That(patch.ErrorMessage, Is.EqualTo("only-db"));
    }

    [Test]
    public void ValidateTaskNames_ValidSourceOnly_ReturnsNull()
    {
        var payload = new RestoreTaskPayload { SourceDatabaseName = "mydb" };
        Assert.That(RestoreTaskHandler.ValidateTaskNames(payload), Is.Null);
    }

    [Test]
    public void ValidateTaskNames_ValidSourceAndTarget_ReturnsNull()
    {
        var payload = new RestoreTaskPayload
        {
            SourceDatabaseName = "mydb",
            TargetDatabaseName = "mydb_restore",
        };
        Assert.That(RestoreTaskHandler.ValidateTaskNames(payload), Is.Null);
    }

    [Test]
    public void ValidateTaskNames_EmptySource_ReturnsError()
    {
        var payload = new RestoreTaskPayload { SourceDatabaseName = "" };
        var error = RestoreTaskHandler.ValidateTaskNames(payload);
        Assert.That(error, Does.Contain("исходной БД"));
    }

    [Test]
    public void ValidateTaskNames_BadCharInSource_ReturnsError()
    {
        var payload = new RestoreTaskPayload { SourceDatabaseName = "foo'; DROP" };
        var error = RestoreTaskHandler.ValidateTaskNames(payload);
        Assert.That(error, Does.Contain("исходной БД"));
        Assert.That(error, Does.Contain("недопустимый символ"));
    }

    [Test]
    public void ValidateTaskNames_BadCharInTarget_ReturnsError()
    {
        var payload = new RestoreTaskPayload
        {
            SourceDatabaseName = "mydb",
            TargetDatabaseName = "../etc/passwd",
        };
        var error = RestoreTaskHandler.ValidateTaskNames(payload);
        Assert.That(error, Does.Contain("целевой БД"));
    }

    [Test]
    public void ValidateTaskNames_NullTarget_IgnoredAsOptional()
    {
        var payload = new RestoreTaskPayload
        {
            SourceDatabaseName = "mydb",
            TargetDatabaseName = null,
        };
        Assert.That(RestoreTaskHandler.ValidateTaskNames(payload), Is.Null);
    }
}
