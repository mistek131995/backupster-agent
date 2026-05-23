using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.State;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Clients;
using BackupsterAgent.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Workers;

[TestFixture]
public sealed class BackupWorkerAutoRebaseTests
{
    private const string Database = "db1";
    private const string Storage = "s3-main";

    private RecordingJobRunner _runner = null!;
    private BackupWorker _worker = null!;

    [SetUp]
    public void SetUp()
    {
        _runner = new RecordingJobRunner();

        var connections = new ConnectionResolver([
            new ConnectionConfig
            {
                Name = "mssql-main",
                DatabaseType = DatabaseType.Mssql,
                Host = "localhost",
                Port = 1433,
            },
        ]);
        var storages = new StorageResolver([
            new StorageConfig { Name = Storage, Provider = UploadProvider.S3, S3 = new S3Settings() },
        ]);
        var databases = Options.Create(new List<DatabaseConfig>
        {
            new() { ConnectionName = "mssql-main", Database = Database, StorageName = Storage },
        });

        _worker = new BackupWorker(
            _runner,
            schedule: null!,
            encryption: null!,
            connections,
            storages,
            new NoopActivityLock(),
            runTracker: null!,
            new StubRecordClient(),
            databases,
            NullLogger<BackupWorker>.Instance);
    }

    [TearDown]
    public void TearDown()
    {
        _worker?.Dispose();
    }

    [Test]
    public async Task ChainBrokenDiff_ThenAutoFullSucceeds_SkipsScheduledPhysicalForSamePair()
    {
        _runner.Queue.Enqueue(BackupResult(success: false, chainBroken: true));
        _runner.Queue.Enqueue(BackupResult(success: true));

        var due = new List<(DatabaseConfig Config, BackupMode Mode, string StorageName, DateTime NextRun)>
        {
            (Db(), BackupMode.PhysicalDifferential, Storage, DateTime.UtcNow),
            (Db(), BackupMode.Physical, Storage, DateTime.UtcNow.AddSeconds(1)),
        };

        await _worker.RunDueDatabasesAsync(due, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_runner.Calls, Has.Count.EqualTo(2),
                "Only DIFF + auto-FULL must run; scheduled Physical must be skipped as covered");
            Assert.That(_runner.Calls[0].Mode, Is.EqualTo(BackupMode.PhysicalDifferential));
            Assert.That(_runner.Calls[1].Mode, Is.EqualTo(BackupMode.Physical));
        });
    }

    [Test]
    public async Task ChainBrokenDiff_AutoFullFails_ScheduledPhysicalStillRunsAsRecovery()
    {
        _runner.Queue.Enqueue(BackupResult(success: false, chainBroken: true));
        _runner.Queue.Enqueue(BackupResult(success: false));
        _runner.Queue.Enqueue(BackupResult(success: true));

        var due = new List<(DatabaseConfig Config, BackupMode Mode, string StorageName, DateTime NextRun)>
        {
            (Db(), BackupMode.PhysicalDifferential, Storage, DateTime.UtcNow),
            (Db(), BackupMode.Physical, Storage, DateTime.UtcNow.AddSeconds(1)),
        };

        await _worker.RunDueDatabasesAsync(due, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_runner.Calls, Has.Count.EqualTo(3),
                "Failed auto-FULL must NOT mark the pair as covered — scheduled Physical must still run");
            Assert.That(_runner.Calls[0].Mode, Is.EqualTo(BackupMode.PhysicalDifferential));
            Assert.That(_runner.Calls[1].Mode, Is.EqualTo(BackupMode.Physical));
            Assert.That(_runner.Calls[2].Mode, Is.EqualTo(BackupMode.Physical));
        });
    }

    [Test]
    public async Task HealthyDiff_NoAutoFull_DoesNotSkipScheduledPhysical()
    {
        _runner.Queue.Enqueue(BackupResult(success: true));
        _runner.Queue.Enqueue(BackupResult(success: true));

        var due = new List<(DatabaseConfig Config, BackupMode Mode, string StorageName, DateTime NextRun)>
        {
            (Db(), BackupMode.PhysicalDifferential, Storage, DateTime.UtcNow),
            (Db(), BackupMode.Physical, Storage, DateTime.UtcNow.AddSeconds(1)),
        };

        await _worker.RunDueDatabasesAsync(due, CancellationToken.None);

        Assert.That(_runner.Calls, Has.Count.EqualTo(2),
            "Successful DIFF must not affect the scheduled Physical that follows");
    }

    private static DatabaseConfig Db() => new()
    {
        ConnectionName = "mssql-main",
        Database = Database,
        StorageName = Storage,
    };

    private static BackupResult BackupResult(bool success, bool chainBroken = false) => new()
    {
        Success = success,
        ChainBroken = chainBroken,
        ErrorMessage = success ? null : "test failure",
        BackupRecordId = Guid.NewGuid(),
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

    private sealed class NoopActivityLock : IAgentActivityLock
    {
        public Task<IDisposable> AcquireAsync(string activityName, CancellationToken ct) =>
            Task.FromResult<IDisposable>(new NoopHandle());

        private sealed class NoopHandle : IDisposable
        {
            public void Dispose() { }
        }
    }

    private sealed class StubRecordClient : IBackupRecordClient
    {
        public Task<OpenRecordResult> OpenAsync(OpenBackupRecordDto dto, CancellationToken ct) =>
            throw new NotSupportedException("StubRecordClient: OpenAsync is not used by RunDueDatabasesAsync");

        public Task ReportProgressAsync(Guid backupRecordId, BackupProgressDto progress, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<FinalizeRecordResult> FinalizeAsync(Guid backupRecordId, FinalizeBackupRecordDto dto, CancellationToken ct) =>
            throw new NotSupportedException("StubRecordClient: FinalizeAsync is not used by RunDueDatabasesAsync");

        public Task<LastSuccessfulLookupResult> GetLastSuccessfulAsync(
            string database, string storage, BackupMode mode, CancellationToken ct) =>
            Task.FromResult(new LastSuccessfulLookupResult(
                LastSuccessfulLookupOutcome.Found,
                new LastSuccessfulBackupResponseDto
                {
                    Id = Guid.NewGuid(),
                    BackupAt = DateTime.UtcNow.AddHours(-1),
                }));
    }
}
