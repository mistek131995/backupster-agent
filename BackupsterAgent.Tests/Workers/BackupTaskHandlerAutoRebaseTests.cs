using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.State;
using BackupsterAgent.Workers.Handlers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Workers;

[TestFixture]
public sealed class BackupTaskHandlerAutoRebaseTests
{
    private RecordingJobRunner _runner = null!;
    private InMemoryTracker _tracker = null!;
    private StorageResolver _storages = null!;
    private BackupTaskHandler _handler = null!;

    [SetUp]
    public void SetUp()
    {
        _runner = new RecordingJobRunner();
        _tracker = new InMemoryTracker();
        _storages = new StorageResolver([
            new StorageConfig { Name = "s3-main", Provider = UploadProvider.S3, S3 = new S3Settings() },
        ]);

        var databases = Options.Create(new List<DatabaseConfig>
        {
            new() { ConnectionName = "mssql-main", Database = "db1", StorageName = "s3-main" },
        });

        _handler = new BackupTaskHandler(
            _runner,
            _tracker,
            _storages,
            databases,
            NullLogger<BackupTaskHandler>.Instance);
    }

    [Test]
    public async Task ChainBroken_TriggersAutoFullAfterFailedDiff_TaskRemainsFailedWithDiffRecordId()
    {
        var diffRecordId = Guid.NewGuid();
        var fullRecordId = Guid.NewGuid();

        _runner.Queue.Enqueue(new BackupResult
        {
            Success = false,
            ChainBroken = true,
            ErrorMessage = "Цепочка сломана",
            BackupRecordId = diffRecordId,
        });
        _runner.Queue.Enqueue(new BackupResult
        {
            Success = true,
            BackupRecordId = fullRecordId,
        });

        var task = MakeBackupTask("db1", BackupMode.PhysicalDifferential, baseRecordId: Guid.NewGuid());

        var patch = await _handler.HandleAsync(task, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_runner.Calls, Has.Count.EqualTo(2),
                "ChainBroken must trigger a second RunAsync call for auto-FULL");
            Assert.That(_runner.Calls[0].Mode, Is.EqualTo(BackupMode.PhysicalDifferential),
                "First call is the original DIFF");
            Assert.That(_runner.Calls[1].Mode, Is.EqualTo(BackupMode.Physical),
                "Second call must be Physical (auto-rebase FULL)");
            Assert.That(_runner.Calls[1].BaseBackupRecordId, Is.Null,
                "Auto-FULL must not reference any base record");

            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Failed),
                "Task status must reflect the failed DIFF, not the rebase FULL");
            Assert.That(patch.Backup!.BackupRecordId, Is.EqualTo(diffRecordId),
                "Task must surface the failed DIFF record id, not the FULL's");
            Assert.That(patch.ErrorMessage, Does.Contain("Цепочка сломана"));
        });
    }

    [Test]
    public async Task SuccessfulDiff_DoesNotTriggerAutoFull()
    {
        var diffRecordId = Guid.NewGuid();
        _runner.Queue.Enqueue(new BackupResult
        {
            Success = true,
            BackupRecordId = diffRecordId,
        });

        var task = MakeBackupTask("db1", BackupMode.PhysicalDifferential, baseRecordId: Guid.NewGuid());

        var patch = await _handler.HandleAsync(task, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_runner.Calls, Has.Count.EqualTo(1),
                "Healthy DIFF must not trigger any extra RunAsync calls");
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Success));
        });
    }

    [Test]
    public async Task FailedDiffWithoutChainBroken_DoesNotTriggerAutoFull()
    {
        _runner.Queue.Enqueue(new BackupResult
        {
            Success = false,
            ChainBroken = false,
            ErrorMessage = "pg_dump failed",
            BackupRecordId = Guid.NewGuid(),
        });

        var task = MakeBackupTask("db1", BackupMode.PhysicalDifferential, baseRecordId: Guid.NewGuid());

        var patch = await _handler.HandleAsync(task, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_runner.Calls, Has.Count.EqualTo(1),
                "Generic failure without ChainBroken flag must not trigger auto-rebase");
            Assert.That(patch.Status, Is.EqualTo(AgentTaskStatus.Failed));
        });
    }

    private static AgentTaskForAgentDto MakeBackupTask(string databaseName, BackupMode mode, Guid baseRecordId) =>
        new()
        {
            Id = Guid.NewGuid(),
            Type = AgentTaskType.Backup,
            Backup = new BackupTaskPayload
            {
                DatabaseName = databaseName,
                BackupMode = mode,
                BaseBackupRecordId = baseRecordId,
                StorageName = "s3-main",
            },
        };

    private sealed class RecordingJobRunner : IBackupJobRunner
    {
        public Queue<BackupResult> Queue { get; } = new();
        public List<(BackupMode Mode, Guid? BaseBackupRecordId)> Calls { get; } = new();

        public Task<BackupResult> RunAsync(
            DatabaseConfig config,
            StorageConfig storage,
            BackupMode mode,
            CancellationToken ct,
            Guid? baseBackupRecordId = null)
        {
            Calls.Add((mode, baseBackupRecordId));
            if (!Queue.TryDequeue(out var next))
                throw new InvalidOperationException(
                    "RecordingJobRunner: no canned BackupResult queued for this call");
            return Task.FromResult(next);
        }
    }

    private sealed class InMemoryTracker : IBackupRunTracker
    {
        private readonly Dictionary<string, DateTime> _store = new();

        public void RecordRun(string key, DateTime whenUtc) => _store[key] = whenUtc;

        public DateTime? GetLastRun(string key) =>
            _store.TryGetValue(key, out var when) ? when : null;
    }
}
