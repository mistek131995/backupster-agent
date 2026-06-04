using System.Security.Cryptography;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Backup.Coordinator;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Services;

public sealed class DatabaseBackupPipelineTests
{
    private string _tempRoot = null!;

    [SetUp]
    public void SetUp()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), "backupster-db-pipeline-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempRoot);
    }

    [TearDown]
    public void TearDown()
    {
        try { if (Directory.Exists(_tempRoot)) Directory.Delete(_tempRoot, recursive: true); }
        catch { }
    }

    [Test]
    public async Task ExecuteAsync_PostgresDifferential_CleansDownloadedBaseManifestWorkDir()
    {
        var encryption = CreateEncryption();
        var parentManifest = Path.Combine(_tempRoot, "parent.backup_manifest");
        await File.WriteAllTextAsync(parentManifest, "{}", CancellationToken.None);
        var encryptedParentManifest = await encryption.EncryptAsync(parentManifest, CancellationToken.None);

        var uploader = new FakeUploadProvider();
        const string parentManifestKey = "db1/2026-01-01_00-00-00/parent.backup_manifest.enc";
        uploader.Downloads[parentManifestKey] = await File.ReadAllBytesAsync(encryptedParentManifest);

        var diffProvider = new RecordingDifferentialBackupProvider();
        var pipeline = BuildPipeline(encryption, uploader, diffProvider);

        var config = new DatabaseConfig
        {
            ConnectionName = "pg-main",
            Database = "db1",
            OutputPath = _tempRoot,
            FilePaths = [],
        };
        var storage = new StorageConfig
        {
            Name = "s3-main",
            Provider = UploadProvider.S3,
            S3 = new S3Settings(),
        };

        var outcome = await pipeline.ExecuteAsync(
            new BackupRunExecution(
                RecordId: Guid.NewGuid(),
                IsOffline: false,
                StartedAt: new DateTime(2026, 01, 01, 12, 00, 00, DateTimeKind.Utc),
                Reporter: TestHelpers.NullReporter<BackupStage>(),
                BasePgBaseManifestKey: parentManifestKey),
            config,
            storage,
            BackupMode.PhysicalDifferential,
            baseBackupRecordId: Guid.NewGuid(),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Success, Is.True);
            Assert.That(diffProvider.BaseManifestPath, Is.Not.Null);
            Assert.That(File.Exists(diffProvider.BaseManifestPath!), Is.False);
            Assert.That(diffProvider.BaseManifestWorkDir, Is.Not.Null);
            Assert.That(Directory.Exists(diffProvider.BaseManifestWorkDir!), Is.False);
            Assert.That(Directory.GetDirectories(_tempRoot, "diff-base-*"), Is.Empty);
        });
    }

    [Test]
    public async Task ExecuteAsync_UnsafeDatabaseName_UsesSafeStorageFolder()
    {
        var encryption = CreateEncryption();
        var parentManifest = Path.Combine(_tempRoot, "parent.backup_manifest");
        await File.WriteAllTextAsync(parentManifest, "{}", CancellationToken.None);
        var encryptedParentManifest = await encryption.EncryptAsync(parentManifest, CancellationToken.None);

        var uploader = new FakeUploadProvider();
        const string parentManifestKey = "legacy/parent.backup_manifest.enc";
        uploader.Downloads[parentManifestKey] = await File.ReadAllBytesAsync(encryptedParentManifest);

        var diffProvider = new RecordingDifferentialBackupProvider();
        var pipeline = BuildPipeline(encryption, uploader, diffProvider);

        var config = new DatabaseConfig
        {
            ConnectionName = "pg-main",
            Database = "../prod",
            OutputPath = _tempRoot,
            FilePaths = [],
        };
        var storage = new StorageConfig
        {
            Name = "s3-main",
            Provider = UploadProvider.S3,
            S3 = new S3Settings(),
        };

        var outcome = await pipeline.ExecuteAsync(
            new BackupRunExecution(
                RecordId: Guid.NewGuid(),
                IsOffline: false,
                StartedAt: new DateTime(2026, 01, 01, 12, 00, 00, DateTimeKind.Utc),
                Reporter: TestHelpers.NullReporter<BackupStage>(),
                BasePgBaseManifestKey: parentManifestKey),
            config,
            storage,
            BackupMode.PhysicalDifferential,
            baseBackupRecordId: Guid.NewGuid(),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Success, Is.True);
            Assert.That(outcome.DumpObjectKey, Does.StartWith("db-Li4vcHJvZA/2026-01-01_12-00-00/"));
            Assert.That(outcome.DumpObjectKey, Does.Not.StartWith("../prod/"));
            Assert.That(diffProvider.DatabaseName, Is.EqualTo("../prod"));
        });
    }

    private DatabaseBackupPipeline BuildPipeline(
        EncryptionService encryption,
        IUploadProvider uploader,
        IDifferentialBackupProvider diffProvider)
    {
        var manifestStore = new ManifestStore(
            encryption,
            Options.Create(new RestoreSettings { TempPath = Path.Combine(_tempRoot, "manifest-temp") }),
            NullLoggerFactory.Instance,
            NullLogger<ManifestStore>.Instance);

        return new DatabaseBackupPipeline(
            new StubBackupProviderFactory(diffProvider),
            new ConnectionResolver([
                new ConnectionConfig
                {
                    Name = "pg-main",
                    DatabaseType = DatabaseType.Postgres,
                    Host = "localhost",
                    Port = 5432,
                },
            ]),
            encryption,
            new StubUploadProviderFactory(uploader),
            new FileBackupService(
                new ContentDefinedChunker(),
                encryption,
                NullLogger<FileBackupService>.Instance),
            manifestStore,
            NullLogger<DatabaseBackupPipeline>.Instance);
    }

    private static EncryptionService CreateEncryption()
    {
        var key = RandomNumberGenerator.GetBytes(32);
        return new EncryptionService(
            Options.Create(new EncryptionSettings { Key = Convert.ToBase64String(key) }),
            NullLogger<EncryptionService>.Instance);
    }

    private sealed class RecordingDifferentialBackupProvider : IDifferentialBackupProvider
    {
        public string? BaseManifestPath { get; private set; }
        public string? BaseManifestWorkDir { get; private set; }
        public string? DatabaseName { get; private set; }

        public Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct)
        {
            DatabaseName = database;
            return Task.CompletedTask;
        }

        public async Task<BackupResult> BackupAsync(
            DatabaseConfig config,
            ConnectionConfig connection,
            DifferentialBackupContext context,
            CancellationToken ct)
        {
            BaseManifestPath = context.BasePgBaseManifestPath;
            BaseManifestWorkDir = Path.GetDirectoryName(context.BasePgBaseManifestPath);

            if (string.IsNullOrWhiteSpace(BaseManifestPath) || !File.Exists(BaseManifestPath))
                throw new InvalidOperationException("Base manifest was not downloaded and decrypted.");

            var dumpPath = Path.Combine(config.OutputPath, $"diff-dump-{Guid.NewGuid():N}.pgbase.tar");
            await File.WriteAllBytesAsync(dumpPath, [1, 2, 3, 4], ct);

            return new BackupResult
            {
                FilePath = dumpPath,
                SizeBytes = 4,
                DurationMs = 10,
                Success = true,
            };
        }
    }

    private sealed class FakeUploadProvider : IUploadProvider
    {
        public Dictionary<string, byte[]> Downloads { get; } = [];
        public Dictionary<string, byte[]> Uploaded { get; } = [];

        public async Task<string> UploadAsync(string filePath, string folder, IProgress<long>? progress, CancellationToken ct)
        {
            var key = $"{folder.TrimEnd('/')}/{Path.GetFileName(filePath)}";
            Uploaded[key] = await File.ReadAllBytesAsync(filePath, ct);
            return $"fake://{key}";
        }

        public Task UploadBytesAsync(byte[] content, string objectKey, CancellationToken ct)
        {
            Uploaded[objectKey] = content;
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string objectKey, CancellationToken ct) =>
            Task.FromResult(Uploaded.ContainsKey(objectKey));

        public Task DownloadAsync(string objectKey, string localPath, IProgress<long>? progress, CancellationToken ct)
        {
            if (!Downloads.TryGetValue(objectKey, out var bytes))
                throw new FileNotFoundException($"No fake download registered for '{objectKey}'.");

            Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
            return File.WriteAllBytesAsync(localPath, bytes, ct);
        }

        public Task<long> GetObjectSizeAsync(string objectKey, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<byte[]> DownloadBytesAsync(string objectKey, CancellationToken ct) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<StorageObject> ListAsync(string prefix, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task DeleteAsync(string objectKey, CancellationToken ct) =>
            throw new NotSupportedException();
    }

    private sealed class StubUploadProviderFactory(IUploadProvider service) : IUploadProviderFactory
    {
        public IUploadProvider GetProvider(string storageName) => service;
    }

    private sealed class StubBackupProviderFactory(IDifferentialBackupProvider diffProvider) : IBackupProviderFactory
    {
        public IBackupProvider GetProvider(DatabaseType databaseType, BackupMode backupMode) =>
            throw new NotSupportedException();

        public IDifferentialBackupProvider GetDifferentialProvider(DatabaseType databaseType) => diffProvider;
    }
}
