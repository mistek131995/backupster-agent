using System.Security.Cryptography;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Backup.Coordinator;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Security;
using BackupsterAgent.Services.Restore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[Category("E2E")]
[NonParallelizable]
public sealed class MongoLogicalBackupPipelineIntegrationTests
{
    private const string StorageName = "localfs-mongo-e2e";

    private ConnectionConfig _connection = null!;
    private ExternalProcessRunner _runner = null!;
    private MongoBinaryResolver _resolver = null!;

    private string _srcDb = null!;
    private string _dstDb = null!;
    private string _outputDir = null!;
    private string _storageDir = null!;
    private string _tempRoot = null!;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(
            IntegrationConfig.TryGetMongoConnection(out var connection),
            Is.True,
            "Mongo:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_MONGO__* env vars.");

        _connection = connection;
        _runner = new ExternalProcessRunner(NullLogger<ExternalProcessRunner>.Instance);
        _resolver = new MongoBinaryResolver(NullLogger<MongoBinaryResolver>.Instance);

        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await MongoIntegrationTestSupport.AssumeMongoToolsAvailableAsync(
            _connection,
            _resolver,
            _runner,
            bootCts.Token);
        await MongoIntegrationTestSupport.DropLeftoverTestDatabasesAsync(_connection, bootCts.Token);
    }

    [SetUp]
    public async Task SetUp()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _srcDb = MongoIntegrationTestSupport.TestDbPrefix + "src_e2e_" + suffix;
        _dstDb = MongoIntegrationTestSupport.TestDbPrefix + "dst_e2e_" + suffix;

        var root = Path.Combine(Path.GetTempPath(), "backupster-mongo-e2e-" + Guid.NewGuid().ToString("N"));
        _outputDir = Path.Combine(root, "out");
        _storageDir = Path.Combine(root, "storage");
        _tempRoot = Path.Combine(root, "temp");

        Directory.CreateDirectory(_outputDir);
        Directory.CreateDirectory(_storageDir);
        Directory.CreateDirectory(_tempRoot);

        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        await MongoIntegrationTestSupport.CreateSourceDatabaseAsync(_connection, _srcDb, _cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await MongoIntegrationTestSupport.DropDatabaseIfExistsAsync(_connection, _srcDb, CancellationToken.None);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Source DB cleanup failed: {ex.Message}");
        }

        try
        {
            await MongoIntegrationTestSupport.DropDatabaseIfExistsAsync(_connection, _dstDb, CancellationToken.None);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Target DB cleanup failed: {ex.Message}");
        }

        _cts?.Dispose();

        try
        {
            var root = Path.GetDirectoryName(_outputDir);
            if (!string.IsNullOrWhiteSpace(root) && Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Local temp cleanup failed: {ex.Message}");
        }
    }

    [Test]
    public async Task PipelineAndRestoreService_RoundTrip_RestoredDatabaseHasContent()
    {
        var encryption = new EncryptionService(
            Options.Create(new EncryptionSettings { Key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)) }),
            NullLogger<EncryptionService>.Instance);
        var restoreSettings = new RestoreSettings { TempPath = _tempRoot };
        var restoreOptions = Options.Create(restoreSettings);
        var databaseConfig = new DatabaseConfig
        {
            ConnectionName = _connection.Name,
            StorageName = StorageName,
            Database = _srcDb,
            OutputPath = _outputDir,
        };
        var storage = new StorageConfig
        {
            Name = StorageName,
            Provider = UploadProvider.LocalFs,
            LocalFs = new LocalFsSettings { RemotePath = _storageDir },
        };
        var uploader = new LocalFsUploadProvider(
            storage.LocalFs,
            NullLogger<LocalFsUploadProvider>.Instance);

        var pipeline = BuildPipeline(encryption, restoreOptions, uploader);
        var exec = new BackupRunExecution(
            RecordId: Guid.NewGuid(),
            IsOffline: false,
            StartedAt: DateTime.UtcNow,
            Reporter: new NullProgressReporter<BackupStage>());

        var outcome = await pipeline.ExecuteAsync(
            exec,
            databaseConfig,
            storage,
            BackupMode.Logical,
            baseBackupRecordId: null,
            _cts.Token);

        Assert.That(outcome.Success, Is.True, outcome.ErrorMessage);
        Assert.That(outcome.DumpObjectKey, Is.Not.Null.And.Not.Empty);
        Assert.That(await uploader.ExistsAsync(outcome.DumpObjectKey!, _cts.Token), Is.True);

        var restoreService = BuildRestoreService(encryption, restoreOptions, databaseConfig);
        var restoreResult = await restoreService.RunAsync(
            Guid.NewGuid(),
            new RestoreTaskPayload
            {
                SourceDatabaseName = _srcDb,
                TargetDatabaseName = _dstDb,
                TargetConnectionName = _connection.Name,
                DumpObjectKey = outcome.DumpObjectKey,
                BackupMode = BackupMode.Logical,
                StorageName = StorageName,
            },
            uploader,
            new NullProgressReporter<RestoreStage>(),
            _cts.Token);

        Assert.That(restoreResult.IsSuccess, Is.True, restoreResult.ErrorMessage);

        var sourceSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _srcDb, _cts.Token);
        var restoredSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _dstDb, _cts.Token);

        Assert.That(restoredSnapshot, Is.EqualTo(sourceSnapshot));
    }

    private DatabaseBackupPipeline BuildPipeline(
        EncryptionService encryption,
        IOptions<RestoreSettings> restoreOptions,
        IUploadProvider uploader)
    {
        var backupProvider = new MongoLogicalBackupProvider(
            NullLogger<MongoLogicalBackupProvider>.Instance,
            _resolver,
            _runner);

        return new DatabaseBackupPipeline(
            new MongoBackupProviderFactory(backupProvider),
            new ConnectionResolver([_connection]),
            encryption,
            new SingleUploadProviderFactory(uploader),
            new FileBackupService(
                new ContentDefinedChunker(),
                encryption,
                NullLogger<FileBackupService>.Instance),
            new ManifestStore(
                encryption,
                restoreOptions,
                NullLoggerFactory.Instance,
                NullLogger<ManifestStore>.Instance),
            NullLogger<DatabaseBackupPipeline>.Instance);
    }

    private DatabaseRestoreService BuildRestoreService(
        EncryptionService encryption,
        IOptions<RestoreSettings> restoreOptions,
        DatabaseConfig databaseConfig)
    {
        var restoreProvider = new MongoRestoreProvider(
            NullLogger<MongoRestoreProvider>.Instance,
            _resolver,
            _runner);

        return new DatabaseRestoreService(
            new ConnectionResolver([_connection]),
            new MongoRestoreProviderFactory(restoreProvider),
            encryption,
            restoreOptions,
            Options.Create(new List<DatabaseConfig> { databaseConfig }),
            NullLogger<DatabaseRestoreService>.Instance);
    }

    private sealed class MongoBackupProviderFactory(IBackupProvider provider) : IBackupProviderFactory
    {
        public IBackupProvider GetProvider(DatabaseType databaseType, BackupMode backupMode)
        {
            if (databaseType == DatabaseType.MongoDb && backupMode == BackupMode.Logical)
                return provider;

            throw new NotSupportedException(
                $"Unexpected backup provider request: DatabaseType='{databaseType}', BackupMode='{backupMode}'.");
        }

        public IDifferentialBackupProvider GetDifferentialProvider(DatabaseType databaseType) =>
            throw new NotSupportedException("Differential backup is not used by MongoDB e2e tests.");
    }

    private sealed class MongoRestoreProviderFactory(IRestoreProvider provider) : IRestoreProviderFactory
    {
        public IRestoreProvider GetProvider(DatabaseType databaseType, BackupMode backupMode)
        {
            if (databaseType == DatabaseType.MongoDb && backupMode == BackupMode.Logical)
                return provider;

            throw new NotSupportedException(
                $"Unexpected restore provider request: DatabaseType='{databaseType}', BackupMode='{backupMode}'.");
        }

        public IDifferentialRestoreProvider GetDifferentialProvider(DatabaseType databaseType) =>
            throw new NotSupportedException("Differential restore is not used by MongoDB e2e tests.");
    }

    private sealed class SingleUploadProviderFactory(IUploadProvider provider) : IUploadProviderFactory
    {
        public IUploadProvider GetProvider(string storageName)
        {
            if (storageName == StorageName)
                return provider;

            throw new NotSupportedException($"Unexpected storage provider request: '{storageName}'.");
        }
    }
}
