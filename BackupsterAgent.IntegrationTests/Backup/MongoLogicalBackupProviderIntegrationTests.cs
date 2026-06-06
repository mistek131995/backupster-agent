using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class MongoLogicalBackupProviderIntegrationTests
{
    private ConnectionConfig _connection = null!;
    private ExternalProcessRunner _runner = null!;
    private MongoBinaryResolver _resolver = null!;

    private string _srcDb = null!;
    private string _dstDb = null!;
    private string _outputDir = null!;
    private DateTime _testStartUtc;
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
        _srcDb = MongoIntegrationTestSupport.TestDbPrefix + "src_" + suffix;
        _dstDb = MongoIntegrationTestSupport.TestDbPrefix + "dst_" + suffix;

        _outputDir = Path.Combine(Path.GetTempPath(), "backupster-mongo-log-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_outputDir);

        _testStartUtc = DateTime.UtcNow;
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
            if (Directory.Exists(_outputDir))
                Directory.Delete(_outputDir, recursive: true);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Local output cleanup failed: {ex.Message}");
        }
    }

    [Test]
    public async Task BackupAsync_RoundTrip_RestoredDatabaseHasContent()
    {
        var provider = new MongoLogicalBackupProvider(
            NullLogger<MongoLogicalBackupProvider>.Instance,
            _resolver,
            _runner);

        var config = new DatabaseConfig
        {
            ConnectionName = _connection.Name,
            StorageName = "n/a",
            Database = _srcDb,
            OutputPath = _outputDir,
        };

        await provider.ValidatePermissionsAsync(_connection, _srcDb, _cts.Token);
        var result = await provider.BackupAsync(config, _connection, _cts.Token);

        Assert.That(result.Success, Is.True);
        Assert.That(File.Exists(result.FilePath), Is.True, "archive.gz file must exist after backup");
        Assert.That(result.SizeBytes, Is.GreaterThan(0));
        Assert.That(
            result.FilePath,
            Does.EndWith(".archive.gz").IgnoreCase,
            "BackupResult.FilePath must use the .archive.gz extension");
        Assert.That(
            Path.GetFullPath(Path.GetDirectoryName(result.FilePath)!),
            Is.EqualTo(Path.GetFullPath(_outputDir)).IgnoreCase,
            "Provider must write the dump under DatabaseConfig.OutputPath");
        Assert.That(
            File.GetLastWriteTimeUtc(result.FilePath),
            Is.GreaterThanOrEqualTo(_testStartUtc.AddSeconds(-2)),
            "Returned FilePath must point to a file produced by this run, not a leftover");

        var archivePath = Path.Combine(_outputDir, "restore.archive");
        await MongoIntegrationTestSupport.DecompressGzAsync(result.FilePath, archivePath, _cts.Token);

        var restoreProvider = new MongoRestoreProvider(
            NullLogger<MongoRestoreProvider>.Instance,
            _resolver,
            _runner);

        await restoreProvider.ValidateRestoreSourceAsync(_connection, archivePath, _cts.Token);
        await restoreProvider.ValidatePermissionsAsync(_connection, _dstDb, _cts.Token);
        await restoreProvider.PrepareTargetDatabaseAsync(_connection, _dstDb, _cts.Token);
        await restoreProvider.RestoreAsync(_connection, _dstDb, _srcDb, archivePath, _cts.Token);

        var sourceSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _srcDb, _cts.Token);
        var restoredSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _dstDb, _cts.Token);

        Assert.That(restoredSnapshot, Is.EqualTo(sourceSnapshot));
    }

    [Test]
    public async Task RestoreAsync_SameDatabase_RestoresOriginalSnapshot()
    {
        var backupProvider = new MongoLogicalBackupProvider(
            NullLogger<MongoLogicalBackupProvider>.Instance,
            _resolver,
            _runner);
        var restoreProvider = new MongoRestoreProvider(
            NullLogger<MongoRestoreProvider>.Instance,
            _resolver,
            _runner);
        var config = CreateDatabaseConfig(_srcDb);

        var originalSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _srcDb, _cts.Token);
        var result = await backupProvider.BackupAsync(config, _connection, _cts.Token);

        await InsertMutationDocumentAsync(_connection, _srcDb, _cts.Token);
        var mutatedSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _srcDb, _cts.Token);
        Assert.That(mutatedSnapshot, Is.Not.EqualTo(originalSnapshot));

        var archivePath = Path.Combine(_outputDir, "restore-same-db.archive");
        await MongoIntegrationTestSupport.DecompressGzAsync(result.FilePath, archivePath, _cts.Token);

        await restoreProvider.ValidateRestoreSourceAsync(_connection, archivePath, _cts.Token);
        await restoreProvider.ValidatePermissionsAsync(_connection, _srcDb, _cts.Token);
        await restoreProvider.PrepareTargetDatabaseAsync(_connection, _srcDb, _cts.Token);
        await restoreProvider.RestoreAsync(_connection, _srcDb, _srcDb, archivePath, _cts.Token);

        var restoredSnapshot = await MongoIntegrationTestSupport.ReadSnapshotAsync(_connection, _srcDb, _cts.Token);
        Assert.That(restoredSnapshot, Is.EqualTo(originalSnapshot));
    }

    [Test]
    public async Task ValidatePermissions_MissingBackupReadRole_FailsClearly()
    {
        Assume.That(
            string.IsNullOrWhiteSpace(_connection.ConnectionUri),
            Is.True,
            "Negative MongoDB permission integration tests require Mongo:Host/Port/Username/Password configuration.");

        var username = MongoIntegrationTestSupport.TestDbPrefix + "no_read_" + Guid.NewGuid().ToString("N")[..8];
        var password = "Backupster-" + Guid.NewGuid().ToString("N");

        Assume.That(
            await TryCreateAdminUserAsync(username, password, roles: [], _cts.Token),
            Is.True,
            "Configured MongoDB user cannot create limited integration test users.");

        try
        {
            var limitedConnection = CopyLegacyConnectionWithCredentials(_connection, username, password);
            var provider = new MongoLogicalBackupProvider(
                NullLogger<MongoLogicalBackupProvider>.Instance,
                _resolver,
                _runner);

            BackupPermissionException? ex = null;
            try
            {
                await provider.ValidatePermissionsAsync(limitedConnection, _srcDb, _cts.Token);
            }
            catch (BackupPermissionException caught)
            {
                ex = caught;
            }

            Assume.That(ex, Is.Not.Null,
                "MongoDB authorization is not enabled or the limited user can read the source database.");
            Assert.That(ex!.Message, Does.Contain("read").IgnoreCase);
            Assert.That(ex.Message, Does.Contain(_srcDb));
        }
        finally
        {
            await DropAdminUserIfExistsAsync(username, CancellationToken.None);
        }
    }

    [Test]
    public async Task ValidatePermissions_MissingRestoreDbOwnerRole_FailsClearly()
    {
        Assume.That(
            string.IsNullOrWhiteSpace(_connection.ConnectionUri),
            Is.True,
            "Negative MongoDB permission integration tests require Mongo:Host/Port/Username/Password configuration.");

        var username = MongoIntegrationTestSupport.TestDbPrefix + "no_restore_" + Guid.NewGuid().ToString("N")[..8];
        var password = "Backupster-" + Guid.NewGuid().ToString("N");
        var roles = new BsonArray
        {
            new BsonDocument
            {
                ["role"] = "read",
                ["db"] = _srcDb,
            },
        };

        Assume.That(
            await TryCreateAdminUserAsync(username, password, roles, _cts.Token),
            Is.True,
            "Configured MongoDB user cannot create limited integration test users.");

        try
        {
            var limitedConnection = CopyLegacyConnectionWithCredentials(_connection, username, password);
            var provider = new MongoRestoreProvider(
                NullLogger<MongoRestoreProvider>.Instance,
                _resolver,
                _runner);

            RestorePermissionException? ex = null;
            try
            {
                await provider.ValidatePermissionsAsync(limitedConnection, _dstDb, _cts.Token);
            }
            catch (RestorePermissionException caught)
            {
                ex = caught;
            }

            Assume.That(ex, Is.Not.Null,
                "MongoDB authorization is not enabled or the limited user can write the target database.");
            Assert.That(ex!.Message, Does.Contain("dbOwner").IgnoreCase);
            Assert.That(ex.Message, Does.Contain(_dstDb));
        }
        finally
        {
            await DropAdminUserIfExistsAsync(username, CancellationToken.None);
        }
    }

    private DatabaseConfig CreateDatabaseConfig(string database) =>
        new()
        {
            ConnectionName = _connection.Name,
            StorageName = "n/a",
            Database = database,
            OutputPath = _outputDir,
        };

    private static async Task InsertMutationDocumentAsync(
        ConnectionConfig connection,
        string dbName,
        CancellationToken ct)
    {
        var db = MongoIntegrationTestSupport.CreateClient(connection).GetDatabase(dbName);
        await db.GetCollection<BsonDocument>("items").InsertOneAsync(
            new BsonDocument
            {
                ["_id"] = 999,
                ["name"] = "after-backup",
            },
            cancellationToken: ct);
    }

    private async Task<bool> TryCreateAdminUserAsync(
        string username,
        string password,
        BsonArray roles,
        CancellationToken ct)
    {
        try
        {
            await DropAdminUserIfExistsAsync(username, ct);
            var adminDb = MongoIntegrationTestSupport.CreateClient(_connection).GetDatabase("admin");
            var command = new BsonDocument
            {
                ["createUser"] = username,
                ["pwd"] = password,
                ["roles"] = roles,
            };

            await adminDb.RunCommandAsync<BsonDocument>(command, cancellationToken: ct);
            return true;
        }
        catch (Exception ex) when (ex is MongoCommandException or MongoAuthenticationException or MongoConnectionException or TimeoutException)
        {
            TestContext.Progress.WriteLine($"Limited MongoDB user setup skipped: {ex.Message}");
            return false;
        }
    }

    private async Task DropAdminUserIfExistsAsync(string username, CancellationToken ct)
    {
        try
        {
            var adminDb = MongoIntegrationTestSupport.CreateClient(_connection).GetDatabase("admin");
            await adminDb.RunCommandAsync<BsonDocument>(new BsonDocument("dropUser", username), cancellationToken: ct);
        }
        catch (MongoCommandException ex) when (ex.CodeName == "UserNotFound" || ex.Code == 11)
        {
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Limited MongoDB user cleanup failed: {ex.Message}");
        }
    }

    private static ConnectionConfig CopyLegacyConnectionWithCredentials(
        ConnectionConfig source,
        string username,
        string password) =>
        new()
        {
            Name = source.Name,
            DatabaseType = source.DatabaseType,
            Host = source.Host,
            Port = source.Port,
            Username = username,
            Password = password,
            BinPath = source.BinPath,
        };

}
