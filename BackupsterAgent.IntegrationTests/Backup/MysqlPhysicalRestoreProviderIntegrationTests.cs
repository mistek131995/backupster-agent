using System.Security.Cryptography;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Security;
using BackupsterAgent.Services.Restore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[Category("XtraBackup")]
[Category("Destructive")]
[Category("MysqlPhysicalRestore")]
[NonParallelizable]
public sealed class MysqlPhysicalRestoreProviderIntegrationTests
{
    private const string TestDbPrefix = "bp_itest_mysql_restore_";
    private const string StorageName = "localfs-mysql-physical-restore";
    private static readonly string[] UserHosts = ["%", "localhost", "127.0.0.1"];

    private ConnectionConfig _connection = null!;
    private ExternalProcessRunner _runner = null!;
    private MysqlBinaryResolver _resolver = null!;
    private string _outputDir = null!;
    private string _storageDir = null!;
    private string _restoreTempDir = null!;
    private string _db1 = null!;
    private string _db2 = null!;
    private string _postBackupDb = null!;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(OperatingSystem.IsLinux(), Is.True,
            "MySQL physical restore through XtraBackup is supported only on Linux.");
        Assume.That(
            IntegrationConfig.TryGetMysqlConnection(out var connection),
            Is.True,
            "Mysql:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_MYSQL__* env vars.");

        _connection = connection;
        _runner = new ExternalProcessRunner(NullLogger<ExternalProcessRunner>.Instance);
        _resolver = new MysqlBinaryResolver(NullLogger<MysqlBinaryResolver>.Instance);

        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await AssumeBinaryAvailableAsync("xtrabackup", bootCts.Token);
        await AssumeBinaryAvailableAsync("xbstream", bootCts.Token);

        var datadir = await QueryDataDirectoryAsync(_connection, bootCts.Token);
        Assume.That(Directory.Exists(datadir), Is.True,
            $"MySQL @@datadir '{datadir}' is not accessible on the agent host.");

        await DropLeftoverTestDatabasesAsync(_connection, bootCts.Token);
    }

    [SetUp]
    public async Task SetUp()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _db1 = TestDbPrefix + "a_" + suffix;
        _db2 = TestDbPrefix + "b_" + suffix;
        _postBackupDb = TestDbPrefix + "post_" + suffix;

        var root = Path.Combine(Path.GetTempPath(), "backupster-mysql-restore-" + Guid.NewGuid().ToString("N"));
        _outputDir = Path.Combine(root, "out");
        _storageDir = Path.Combine(root, "storage");
        _restoreTempDir = Path.Combine(root, "restore-temp");
        Directory.CreateDirectory(_outputDir);
        Directory.CreateDirectory(_storageDir);
        Directory.CreateDirectory(_restoreTempDir);

        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(20));

        await WaitForMysqlAvailableAsync(_connection, _cts.Token);
        await CreateSourceDatabaseAsync(_connection, _db1, "alpha", _cts.Token);
        await CreateSourceDatabaseAsync(_connection, _db2, "bravo", _cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            using var cleanupCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
            await WaitForMysqlAvailableAsync(_connection, cleanupCts.Token);
            await DropDatabaseIfExistsAsync(_connection, _db1, cleanupCts.Token);
            await DropDatabaseIfExistsAsync(_connection, _db2, cleanupCts.Token);
            await DropDatabaseIfExistsAsync(_connection, _postBackupDb, cleanupCts.Token);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"MySQL restore test cleanup failed: {ex.Message}");
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
    public async Task RestoreAsync_RoundTrip_RestoresWholeDatadir()
    {
        var backupPath = await CreatePhysicalBackupAsync(_cts.Token);
        var expected = await ReadRestoreSnapshotAsync(_connection, _cts.Token);

        await MutateAfterBackupAsync(_connection, _cts.Token);
        var mutated = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        Assert.That(mutated, Is.Not.EqualTo(expected));

        var provider = BuildRestoreProvider();
        await provider.ValidatePermissionsAsync(_connection, _db1, _cts.Token);
        await provider.ValidateRestoreSourceAsync(_connection, backupPath, _cts.Token);
        await provider.PrepareTargetDatabaseAsync(_connection, _db1, _cts.Token);
        await provider.RestoreAsync(_connection, _db1, _db1, backupPath, _cts.Token);

        await WaitForMysqlAvailableAsync(_connection, _cts.Token);
        var restored = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        Assert.That(restored, Is.EqualTo(expected));
    }

    [Test]
    public async Task DatabaseRestoreService_PhysicalMysql_RoundTrip_FromLocalFsEncryptedDump()
    {
        var backupPath = await CreatePhysicalBackupAsync(_cts.Token);
        var expected = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        var encryption = CreateEncryptionService();
        var encryptedPath = await encryption.EncryptAsync(backupPath, _cts.Token);
        var uploader = CreateLocalFsUploadProvider();
        var folder = $"{_db1}/{DateTime.UtcNow:yyyy-MM-dd_HH-mm-ss}";
        await uploader.UploadAsync(encryptedPath, folder, progress: null, _cts.Token);
        var objectKey = $"{folder}/{Path.GetFileName(encryptedPath)}";

        await MutateAfterBackupAsync(_connection, _cts.Token);

        var service = BuildRestoreService(encryption);
        var taskId = Guid.NewGuid();
        var result = await service.RunAsync(
            taskId,
            new RestoreTaskPayload
            {
                SourceDatabaseName = _db1,
                TargetConnectionName = _connection.Name,
                DumpObjectKey = objectKey,
                BackupMode = BackupMode.Physical,
                StorageName = StorageName,
            },
            uploader,
            new NullProgressReporter<RestoreStage>(),
            _cts.Token);

        Assert.That(result.IsSuccess, Is.True, result.ErrorMessage);
        await WaitForMysqlAvailableAsync(_connection, _cts.Token);
        var restored = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        Assert.That(restored, Is.EqualTo(expected));
        Assert.That(
            Directory.Exists(DatabaseRestoreService.BuildTempDir(_restoreTempDir, taskId)),
            Is.False);
    }

    [Test]
    public async Task DatabaseRestoreService_PhysicalMysql_TargetRenameRejectedBeforeRestore()
    {
        var service = BuildRestoreService(CreateEncryptionService());
        var uploader = CreateLocalFsUploadProvider();

        var result = await service.RunAsync(
            Guid.NewGuid(),
            new RestoreTaskPayload
            {
                SourceDatabaseName = _db1,
                TargetDatabaseName = _db1 + "_renamed",
                TargetConnectionName = _connection.Name,
                DumpObjectKey = "missing.enc",
                BackupMode = BackupMode.Physical,
                StorageName = StorageName,
            },
            uploader,
            new NullProgressReporter<RestoreStage>(),
            _cts.Token);

        Assert.That(result.IsSuccess, Is.False);
        Assert.That(result.ErrorMessage, Does.Contain("Physical restore MySQL"));
        await WaitForMysqlAvailableAsync(_connection, _cts.Token);
        Assert.That(await DatabaseExistsAsync(_connection, _db1, _cts.Token), Is.True);
    }

    [Test]
    public async Task RestoreAsync_CorruptedArchive_FailsBeforeStoppingMysql_AndCleansStaging()
    {
        var datadir = await QueryDataDirectoryAsync(_connection, _cts.Token);
        var (parent, leaf) = MysqlDatadirSwapper.SplitPath(Path.GetFullPath(datadir));
        var beforeStaging = SnapshotStagingDirs(parent, leaf);
        var corruptedPath = Path.Combine(_outputDir, "corrupted.xbstream.gz");
        await File.WriteAllBytesAsync(corruptedPath, [0x1f, 0x8b, 0x08, 0x00, 0x00], _cts.Token);
        var expected = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        var provider = BuildRestoreProvider();

        var ex = Assert.CatchAsync<Exception>(() =>
            provider.RestoreAsync(_connection, _db1, _db1, corruptedPath, _cts.Token));

        Assert.That(ex!.Message, Is.Not.Empty);
        await WaitForMysqlAvailableAsync(_connection, _cts.Token);
        var after = await ReadRestoreSnapshotAsync(_connection, _cts.Token);
        Assert.That(after, Is.EqualTo(expected));
        var leaked = SnapshotStagingDirs(parent, leaf).Except(beforeStaging).ToArray();
        Assert.That(leaked, Is.Empty);
    }

    [Test]
    public async Task ValidatePermissions_UnmanagedMysql_MissingShutdownPrivilege_FailsClearly()
    {
        var inspector = new MysqlInstanceInspector(
            NullLogger<MysqlInstanceInspector>.Instance,
            new MysqlServerProbe(NullLogger<MysqlServerProbe>.Instance),
            new SystemdUnitDetector(NullLogger<SystemdUnitDetector>.Instance),
            new MysqlSystemdController(new SystemdServiceController(
                NullLogger<SystemdServiceController>.Instance,
                _runner,
                Options.Create(new RestoreSettings()))));
        var serviceName = await inspector.DetectServiceNameAsync(_connection, _cts.Token);
        Assume.That(serviceName, Is.Null,
            "Missing SHUTDOWN integration test applies only to unmanaged MySQL processes.");

        var username = TestDbPrefix + "no_shutdown_" + Guid.NewGuid().ToString("N")[..8];
        var password = "Backupster-" + Guid.NewGuid().ToString("N");
        Assume.That(
            await TryCreateBackupOnlyUserAsync(username, password, _cts.Token),
            Is.True,
            "Configured Mysql user cannot create limited integration test users.");

        try
        {
            var limited = CopyConnectionWithCredentials(_connection, username, password);
            var provider = BuildRestoreProvider();

            var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
                provider.ValidatePermissionsAsync(limited, _db1, _cts.Token));

            Assert.That(ex!.Message, Does.Contain("SHUTDOWN"));
        }
        finally
        {
            await DropUserIfExistsAsync(username, CancellationToken.None);
        }
    }

    private async Task<string> CreatePhysicalBackupAsync(CancellationToken ct)
    {
        var provider = new MysqlPhysicalBackupProvider(
            NullLogger<MysqlPhysicalBackupProvider>.Instance,
            _resolver,
            _runner);
        var config = new DatabaseConfig
        {
            ConnectionName = _connection.Name,
            StorageName = "n/a",
            Database = _db1,
            OutputPath = _outputDir,
        };

        await provider.ValidatePermissionsAsync(_connection, _db1, ct);
        var result = await provider.BackupAsync(config, _connection, ct);
        Assert.That(File.Exists(result.FilePath), Is.True);
        return result.FilePath;
    }

    private MysqlPhysicalRestoreProvider BuildRestoreProvider()
    {
        var restoreOptions = Options.Create(new RestoreSettings
        {
            TempPath = _restoreTempDir,
            ChownTimeoutSeconds = 1800,
            SystemctlTimeoutSeconds = 60,
            SystemctlStopStartTimeoutSeconds = 1800,
        });
        var probe = new MysqlServerProbe(NullLogger<MysqlServerProbe>.Instance);
        var extractor = new MysqlBackupExtractor(NullLogger<MysqlBackupExtractor>.Instance, _runner);
        var systemd = new MysqlSystemdController(new SystemdServiceController(
            NullLogger<SystemdServiceController>.Instance,
            _runner,
            restoreOptions));
        var lifecycle = new MysqlLifecycleManager(
            NullLogger<MysqlLifecycleManager>.Instance,
            probe,
            systemd,
            _resolver);
        var swapper = new MysqlDatadirSwapper(
            NullLogger<MysqlDatadirSwapper>.Instance,
            restoreOptions);
        var inspector = new MysqlInstanceInspector(
            NullLogger<MysqlInstanceInspector>.Instance,
            probe,
            new SystemdUnitDetector(NullLogger<SystemdUnitDetector>.Instance),
            systemd);
        var coordinator = new MysqlDatadirSwapCoordinator(
            NullLogger<MysqlDatadirSwapCoordinator>.Instance,
            swapper,
            lifecycle,
            probe);

        return new MysqlPhysicalRestoreProvider(
            NullLogger<MysqlPhysicalRestoreProvider>.Instance,
            _resolver,
            probe,
            extractor,
            inspector,
            lifecycle,
            swapper,
            coordinator);
    }

    private DatabaseRestoreService BuildRestoreService(EncryptionService encryption) =>
        new(
            new ConnectionResolver([_connection]),
            new MysqlRestoreProviderFactory(BuildRestoreProvider()),
            encryption,
            Options.Create(new RestoreSettings { TempPath = _restoreTempDir }),
            Options.Create(new List<DatabaseConfig>
            {
                new()
                {
                    ConnectionName = _connection.Name,
                    StorageName = StorageName,
                    Database = _db1,
                    OutputPath = _outputDir,
                },
            }),
            NullLogger<DatabaseRestoreService>.Instance);

    private EncryptionService CreateEncryptionService() =>
        new(
            Options.Create(new EncryptionSettings
            {
                Key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)),
            }),
            NullLogger<EncryptionService>.Instance);

    private LocalFsUploadProvider CreateLocalFsUploadProvider() =>
        new(
            new LocalFsSettings { RemotePath = _storageDir },
            NullLogger<LocalFsUploadProvider>.Instance);

    private async Task AssumeBinaryAvailableAsync(string binaryName, CancellationToken ct)
    {
        var binary = _resolver.Resolve(_connection, binaryName);
        try
        {
            var result = await _runner.RunAsync(
                new ExternalProcessRequest
                {
                    FileName = binary,
                    Arguments = ["--version"],
                },
                handleStdout: null,
                handleStdin: null,
                ct);
            Assume.That(result.ExitCode, Is.Zero,
                $"{binaryName} --version returned {result.ExitCode}; configure Mysql:BinPath or PATH.");
        }
        catch (Exception ex)
        {
            Assume.That(false, $"{binaryName} is not available: {ex.Message}");
        }
    }

    private async Task MutateAfterBackupAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await ExecuteOnDatabaseAsync(
            connection,
            _db1,
            "UPDATE items SET name = 'after-backup' WHERE id = 1;",
            ct);
        await CreateSourceDatabaseAsync(connection, _postBackupDb, "post", ct);
    }

    private async Task<Dictionary<string, string>> ReadRestoreSnapshotAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var db in new[] { _db1, _db2, _postBackupDb })
        {
            if (!await DatabaseExistsAsync(connection, db, ct))
            {
                result[db] = "<missing>";
                continue;
            }

            result[db] = await ReadItemsAsync(connection, db, ct);
        }
        return result;
    }

    private static async Task CreateSourceDatabaseAsync(
        ConnectionConfig connection,
        string dbName,
        string marker,
        CancellationToken ct)
    {
        await ExecuteOnServerAsync(
            connection,
            $"CREATE DATABASE `{EscapeIdentifier(dbName)}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
            ct);

        const string ddl = @"
CREATE TABLE items (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";
        await ExecuteOnDatabaseAsync(connection, dbName, ddl, ct);
        await ExecuteOnDatabaseAsync(
            connection,
            dbName,
            $"INSERT INTO items (id, name) VALUES (1, '{EscapeSqlString(marker)}'), (2, 'stable');",
            ct);
    }

    private static async Task<string> ReadItemsAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var conn = new MySqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand("SELECT id, name FROM items ORDER BY id;", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add($"{reader.GetInt32(0)}:{reader.GetString(1)}");
        return string.Join("|", rows);
    }

    private static async Task<bool> DatabaseExistsAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = @name;",
            conn);
        cmd.Parameters.AddWithValue("@name", dbName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) > 0;
    }

    private static async Task WaitForMysqlAvailableAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(120);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var conn = new MySqlConnection(BuildServerConnectionString(connection));
                await conn.OpenAsync(ct);
                await using var cmd = new MySqlCommand("SELECT 1;", conn);
                await cmd.ExecuteScalarAsync(ct);
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(1000, ct);
            }
        }

        throw new InvalidOperationException("MySQL did not become available after restore.", last);
    }

    private static async Task<string> QueryDataDirectoryAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand("SELECT @@datadir;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);
        return (result as string ?? string.Empty)
            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
    }

    private static async Task DropDatabaseIfExistsAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(dbName)) return;
        await ExecuteOnServerAsync(connection, $"DROP DATABASE IF EXISTS `{EscapeIdentifier(dbName)}`;", ct);
    }

    private static async Task DropLeftoverTestDatabasesAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var leftovers = new List<string>();
        await using (var conn = new MySqlConnection(BuildServerConnectionString(connection)))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new MySqlCommand(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE @prefix;",
                conn);
            cmd.Parameters.AddWithValue("@prefix", TestDbPrefix + "%");
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                leftovers.Add(reader.GetString(0));
        }

        foreach (var name in leftovers)
        {
            try { await DropDatabaseIfExistsAsync(connection, name, ct); }
            catch (Exception ex)
            {
                TestContext.Progress.WriteLine($"Leftover DB '{name}' cleanup failed: {ex.Message}");
            }
        }
    }

    private async Task<bool> TryCreateBackupOnlyUserAsync(string username, string password, CancellationToken ct)
    {
        try
        {
            await DropUserIfExistsAsync(username, ct);
            foreach (var host in UserHosts)
            {
                await ExecuteOnServerAsync(
                    _connection,
                    $"CREATE USER '{EscapeSqlString(username)}'@'{EscapeSqlString(host)}' IDENTIFIED BY '{EscapeSqlString(password)}';",
                    ct);
                await ExecuteOnServerAsync(
                    _connection,
                    $"GRANT RELOAD, PROCESS, REPLICATION CLIENT ON *.* TO '{EscapeSqlString(username)}'@'{EscapeSqlString(host)}';",
                    ct);
            }

            await ExecuteOnServerAsync(_connection, "FLUSH PRIVILEGES;", ct);
            return true;
        }
        catch (MySqlException ex)
        {
            TestContext.Progress.WriteLine($"Limited user setup failed: {ex.Message}");
            return false;
        }
    }

    private async Task DropUserIfExistsAsync(string username, CancellationToken ct)
    {
        try
        {
            foreach (var host in UserHosts)
            {
                await ExecuteOnServerAsync(
                    _connection,
                    $"DROP USER IF EXISTS '{EscapeSqlString(username)}'@'{EscapeSqlString(host)}';",
                    ct);
            }
            await ExecuteOnServerAsync(_connection, "FLUSH PRIVILEGES;", ct);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Limited user cleanup failed: {ex.Message}");
        }
    }

    private static HashSet<string> SnapshotStagingDirs(string parent, string leaf)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var suffix in new[] { "new", "old", "failed" })
        {
            try
            {
                foreach (var dir in Directory.EnumerateDirectories(parent, $"{leaf}.{suffix}-*"))
                    result.Add(Path.GetFullPath(dir));
            }
            catch
            {
            }
        }
        return result;
    }

    private static async Task ExecuteOnServerAsync(ConnectionConfig connection, string sql, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteOnDatabaseAsync(
        ConnectionConfig connection,
        string dbName,
        string sql,
        CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static ConnectionConfig CopyConnectionWithCredentials(
        ConnectionConfig source,
        string username,
        string password) =>
        new()
        {
            Name = source.Name,
            DatabaseType = source.DatabaseType,
            ConnectionUri = source.ConnectionUri,
            Host = source.Host,
            Port = source.Port,
            Username = username,
            Password = password,
            BinPath = source.BinPath,
        };

    private static string BuildServerConnectionString(ConnectionConfig connection) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
        }.ToString();

    private static string BuildConnectionString(ConnectionConfig connection, string database) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
            Database = database,
        }.ToString();

    private static string EscapeIdentifier(string identifier) => identifier.Replace("`", "``");

    private static string EscapeSqlString(string value) => value.Replace("'", "''");

    private sealed class MysqlRestoreProviderFactory(MysqlPhysicalRestoreProvider physical) : IRestoreProviderFactory
    {
        public IRestoreProvider GetProvider(DatabaseType databaseType, BackupMode backupMode)
        {
            if (databaseType == DatabaseType.Mysql && backupMode == BackupMode.Physical)
                return physical;

            throw new NotSupportedException(
                $"Unexpected restore provider request: DatabaseType='{databaseType}', BackupMode='{backupMode}'.");
        }

        public IDifferentialRestoreProvider GetDifferentialProvider(DatabaseType databaseType) =>
            throw new NotSupportedException("Differential restore is not used by MySQL physical restore integration tests.");
    }
}
