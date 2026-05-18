using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Workers.Handlers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Workers;

[TestFixture]
public sealed class HandlerRoutingTests
{
    [Test]
    public void RestoreHandler_AcceptsRestore_RejectsOthers()
    {
        var handler = CreateRestoreHandler();

        Assert.Multiple(() =>
        {
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Restore)), Is.True);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Delete)), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Backup)), Is.False);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "fs")), Is.False);
        });
    }

    [Test]
    public void DeleteHandler_AcceptsDelete_RejectsOthers()
    {
        var handler = CreateDeleteHandler();

        Assert.Multiple(() =>
        {
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Delete)), Is.True);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Restore)), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Backup)), Is.False);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "fs")), Is.False);
        });
    }

    [Test]
    public void BackupHandler_AcceptsBackupWithoutFileSet_RejectsBackupWithFileSet()
    {
        var handler = CreateBackupHandler();

        Assert.Multiple(() =>
        {
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: null)), Is.True);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "")), Is.True);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "   ")), Is.True);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Backup)), Is.True);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "fs")), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Restore)), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Delete)), Is.False);
        });
    }

    [Test]
    public void FileSetHandler_AcceptsBackupWithFileSet_RejectsBackupWithoutFileSet()
    {
        var handler = CreateFileSetHandler();

        Assert.Multiple(() =>
        {
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "fs")), Is.True);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: null)), Is.False);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "")), Is.False);
            Assert.That(handler.CanHandle(MakeBackupTask(fileSetName: "   ")), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Backup)), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Restore)), Is.False);
            Assert.That(handler.CanHandle(MakeTask(AgentTaskType.Delete)), Is.False);
        });
    }

    [TestCase(AgentTaskType.Restore, null, typeof(RestoreTaskHandler))]
    [TestCase(AgentTaskType.Delete, null, typeof(DeleteTaskHandler))]
    [TestCase(AgentTaskType.Backup, null, typeof(BackupTaskHandler))]
    [TestCase(AgentTaskType.Backup, "weekly-photos", typeof(FileSetBackupTaskHandler))]
    public void ExactlyOneHandlerAcceptsTask(AgentTaskType type, string? fileSetName, Type expectedHandler)
    {
        var task = type == AgentTaskType.Backup
            ? MakeBackupTask(fileSetName)
            : MakeTask(type);
        var handlers = AllHandlers();

        var accepting = handlers.Where(h => h.CanHandle(task)).ToList();

        Assert.That(accepting, Has.Count.EqualTo(1));
        Assert.That(accepting[0], Is.InstanceOf(expectedHandler));
    }

    private static IReadOnlyList<IAgentTaskHandler> AllHandlers() => new IAgentTaskHandler[]
    {
        CreateRestoreHandler(),
        CreateDeleteHandler(),
        CreateBackupHandler(),
        CreateFileSetHandler(),
    };

    private static AgentTaskForAgentDto MakeTask(AgentTaskType type) =>
        new() { Id = Guid.NewGuid(), Type = type };

    private static AgentTaskForAgentDto MakeBackupTask(string? fileSetName) => new()
    {
        Id = Guid.NewGuid(),
        Type = AgentTaskType.Backup,
        Backup = new BackupTaskPayload { DatabaseName = "db", FileSetName = fileSetName },
    };

    private static RestoreTaskHandler CreateRestoreHandler() =>
        new(null!, null!, null!, null!,
            Options.Create(new List<DatabaseConfig>()),
            NullLogger<RestoreTaskHandler>.Instance);

    private static DeleteTaskHandler CreateDeleteHandler() =>
        new(null!, null!, NullLogger<DeleteTaskHandler>.Instance);

    private static BackupTaskHandler CreateBackupHandler() =>
        new(null!, null!, null!,
            Options.Create(new List<DatabaseConfig>()),
            NullLogger<BackupTaskHandler>.Instance);

    private static FileSetBackupTaskHandler CreateFileSetHandler() =>
        new(null!, null!, null!,
            Options.Create(new List<FileSetConfig>()),
            NullLogger<FileSetBackupTaskHandler>.Instance);
}
