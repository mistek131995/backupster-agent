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

    [Test]
    public void BackupTask_WithoutFileSet_RoutedToBackupHandlerOnly()
    {
        var task = MakeBackupTask(fileSetName: null);
        var handlers = AllHandlers();

        var accepting = handlers.Where(h => h.CanHandle(task)).ToList();

        Assert.That(accepting, Has.Count.EqualTo(1));
        Assert.That(accepting[0], Is.InstanceOf<BackupTaskHandler>());
    }

    [Test]
    public void BackupTask_WithFileSet_RoutedToFileSetHandlerOnly()
    {
        var task = MakeBackupTask(fileSetName: "weekly-photos");
        var handlers = AllHandlers();

        var accepting = handlers.Where(h => h.CanHandle(task)).ToList();

        Assert.That(accepting, Has.Count.EqualTo(1));
        Assert.That(accepting[0], Is.InstanceOf<FileSetBackupTaskHandler>());
    }

    [Test]
    public void RestoreTask_RoutedToRestoreHandlerOnly()
    {
        var task = MakeTask(AgentTaskType.Restore);
        var handlers = AllHandlers();

        var accepting = handlers.Where(h => h.CanHandle(task)).ToList();

        Assert.That(accepting, Has.Count.EqualTo(1));
        Assert.That(accepting[0], Is.InstanceOf<RestoreTaskHandler>());
    }

    [Test]
    public void DeleteTask_RoutedToDeleteHandlerOnly()
    {
        var task = MakeTask(AgentTaskType.Delete);
        var handlers = AllHandlers();

        var accepting = handlers.Where(h => h.CanHandle(task)).ToList();

        Assert.That(accepting, Has.Count.EqualTo(1));
        Assert.That(accepting[0], Is.InstanceOf<DeleteTaskHandler>());
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
        new(null!, null!,
            Options.Create(new List<DatabaseConfig>()),
            NullLogger<BackupTaskHandler>.Instance);

    private static FileSetBackupTaskHandler CreateFileSetHandler() =>
        new(null!, null!,
            Options.Create(new List<FileSetConfig>()),
            NullLogger<FileSetBackupTaskHandler>.Instance);
}
