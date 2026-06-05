using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[Category("XtraBackup")]
[NonParallelizable]
public sealed class MysqlPhysicalBackupProviderIntegrationTests
{
    private const string TestDbPrefix = "bp_itest_mysql_phys_";
    private const string LimitedUserPrefix = "bp_itest_xtra_limited_";

    private ConnectionConfig _connection = null!;
    private ExternalProcessRunner _runner = null!;
    private MysqlBinaryResolver _resolver = null!;

    private string _srcDb = null!;
    private string _outputDir = null!;
    private DateTime _testStartUtc;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(OperatingSystem.IsLinux(), Is.True,
            "MySQL physical backup through XtraBackup is supported only on Linux.");
        Assume.That(
            IntegrationConfig.TryGetMysqlConnection(out var connection),
            Is.True,
            "Mysql:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_MYSQL__* env vars.");

        _connection = connection;
        _runner = new ExternalProcessRunner(NullLogger<ExternalProcessRunner>.Instance);
        _resolver = new MysqlBinaryResolver(NullLogger<MysqlBinaryResolver>.Instance);

        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var xtrabackup = _resolver.Resolve(_connection, "xtrabackup");
        Assume.That(await IsBinaryAvailableAsync(xtrabackup, bootCts.Token), Is.True,
            "xtrabackup is not available for configured Mysql:BinPath/PATH.");

        var xbstream = _resolver.Resolve(_connection, "xbstream");
        Assume.That(await IsBinaryAvailableAsync(xbstream, bootCts.Token), Is.True,
            "xbstream is not available for configured Mysql:BinPath/PATH.");

        var datadir = await QueryDataDirectoryAsync(_connection, bootCts.Token);
        Assume.That(Directory.Exists(datadir), Is.True,
            $"MySQL @@datadir '{datadir}' is not accessible on the agent host.");

        await DropLeftoverTestDatabasesAsync(_connection, bootCts.Token);
    }

    [SetUp]
    public async Task SetUp()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _srcDb = TestDbPrefix + "src_" + suffix;

        _outputDir = Path.Combine(Path.GetTempPath(), "backupster-mysql-phys-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_outputDir);

        _testStartUtc = DateTime.UtcNow;
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));

        var created = await TryCreateSourceDatabaseAsync(_connection, _srcDb, _cts.Token);
        Assume.That(created, Is.True,
            "Configured Mysql user cannot create/drop integration test databases.");
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await DropDatabaseIfExistsAsync(_connection, _srcDb, CancellationToken.None);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Source DB cleanup failed: {ex.Message}");
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
    public async Task ValidatePermissions_WithRequiredPrivileges_Passes()
    {
        var provider = CreateBackupProvider();

        await provider.ValidatePermissionsAsync(_connection, _srcDb, _cts.Token);
    }

    [Test]
    public async Task ValidatePermissions_MissingPhysicalBackupPrivileges_FailsClearly()
    {
        var limitedUser = LimitedUserPrefix + Guid.NewGuid().ToString("N")[..8];
        var limitedPassword = "Backupster-" + Guid.NewGuid().ToString("N");

        Assume.That(
            await TryCreateLimitedUserAsync(_connection, limitedUser, limitedPassword, _cts.Token),
            Is.True,
            "Configured Mysql user cannot create limited integration test users.");

        try
        {
            var limitedConnection = CopyConnection(_connection, limitedUser, limitedPassword);
            var provider = CreateBackupProvider();

            var ex = Assert.ThrowsAsync<BackupPermissionException>(() =>
                provider.ValidatePermissionsAsync(limitedConnection, _srcDb, _cts.Token));

            Assert.That(ex!.Message, Does.Contain("RELOAD"));
            Assert.That(ex.Message, Does.Contain("PROCESS"));
            Assert.That(ex.Message, Does.Contain("REPLICATION CLIENT"));
        }
        finally
        {
            await DropUserIfExistsAsync(_connection, limitedUser, CancellationToken.None);
        }
    }

    [Test]
    public async Task BackupAsync_CreatesValidPreparedXbstreamArchive()
    {
        var provider = CreateBackupProvider();
        var config = CreateDatabaseConfig();

        var result = await provider.BackupAsync(config, _connection, _cts.Token);

        Assert.That(result.Success, Is.True);
        Assert.That(File.Exists(result.FilePath), Is.True, "xbstream.gz file must exist after backup");
        Assert.That(result.SizeBytes, Is.GreaterThan(0));
        Assert.That(
            result.FilePath,
            Does.EndWith(".xbstream.gz").IgnoreCase,
            "BackupResult.FilePath must use the .xbstream.gz extension");
        Assert.That(
            Path.GetFullPath(Path.GetDirectoryName(result.FilePath)!),
            Is.EqualTo(Path.GetFullPath(_outputDir)).IgnoreCase,
            "Provider must write the physical dump under DatabaseConfig.OutputPath");
        Assert.That(
            File.GetLastWriteTimeUtc(result.FilePath),
            Is.GreaterThanOrEqualTo(_testStartUtc.AddSeconds(-2)),
            "Returned FilePath must point to a file produced by this run, not a leftover");
        AssertNoXtraTempDirs();

        var extractDir = Path.Combine(_outputDir, "extract");
        Directory.CreateDirectory(extractDir);

        var extractor = CreateBackupExtractor();
        await extractor.ExtractXbstreamAsync(_resolver.Resolve(_connection, "xbstream"), result.FilePath, extractDir, _cts.Token);
        await extractor.PrepareBackupAsync(_resolver.Resolve(_connection, "xtrabackup"), extractDir, _cts.Token);

        var checkpointsPath = Path.Combine(extractDir, "xtrabackup_checkpoints");
        Assert.That(File.Exists(checkpointsPath), Is.True, "Prepared backup must contain xtrabackup_checkpoints");
        Assert.That(await File.ReadAllTextAsync(checkpointsPath, _cts.Token), Does.Contain("backup_type = full-prepared"));
        Assert.That(Directory.Exists(Path.Combine(extractDir, _srcDb)), Is.True,
            "Physical archive must include the source database directory.");
    }

    [Test]
    public async Task BackupAsync_WhenXtrabackupFails_CleansPartialFileAndTempDir()
    {
        var provider = CreateBackupProvider();
        var config = CreateDatabaseConfig();
        var badConnection = CopyConnection(
            _connection,
            "backupster_missing_" + Guid.NewGuid().ToString("N")[..8],
            "invalid-password-" + Guid.NewGuid().ToString("N"));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.BackupAsync(config, badConnection, _cts.Token));

        Assert.That(ex!.Message, Does.Contain("xtrabackup"));
        Assert.That(Directory.GetFiles(_outputDir, "*.xbstream.gz"), Is.Empty);
        AssertNoXtraTempDirs();
    }

    private MysqlPhysicalBackupProvider CreateBackupProvider() =>
        new(
            NullLogger<MysqlPhysicalBackupProvider>.Instance,
            _resolver,
            _runner);

    private MysqlBackupExtractor CreateBackupExtractor() =>
        new(
            NullLogger<MysqlBackupExtractor>.Instance,
            _runner);

    private DatabaseConfig CreateDatabaseConfig() =>
        new()
        {
            ConnectionName = _connection.Name,
            StorageName = "n/a",
            Database = _srcDb,
            OutputPath = _outputDir,
        };

    private void AssertNoXtraTempDirs()
    {
        Assert.That(
            Directory.EnumerateDirectories(_outputDir, "xtra-*"),
            Is.Empty,
            "Provider must clean XtraBackup temp target directories.");
    }

    private async Task<bool> IsBinaryAvailableAsync(string binary, CancellationToken ct)
    {
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
            return result.ExitCode == 0;
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Binary check failed for '{binary}': {ex.Message}");
            return false;
        }
    }

    private static async Task<bool> TryCreateSourceDatabaseAsync(
        ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        try
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

            foreach (var (id, name) in ExpectedRows)
            {
                await using var conn = new MySqlConnection(BuildConnectionString(connection, dbName));
                await conn.OpenAsync(ct);
                await using var cmd = new MySqlCommand("INSERT INTO items (id, name) VALUES (@id, @name);", conn);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@name", name);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            return true;
        }
        catch (MySqlException ex)
        {
            TestContext.Progress.WriteLine($"Source DB setup failed: {ex.Message}");
            return false;
        }
    }

    private static readonly (int Id, string Name)[] ExpectedRows =
    [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ];

    private static async Task<bool> TryCreateLimitedUserAsync(
        ConnectionConfig connection, string username, string password, CancellationToken ct)
    {
        try
        {
            await DropUserIfExistsAsync(connection, username, ct);
            foreach (var host in LimitedUserHosts)
            {
                await ExecuteOnServerAsync(
                    connection,
                    $"CREATE USER '{EscapeSqlString(username)}'@'{EscapeSqlString(host)}' IDENTIFIED BY '{EscapeSqlString(password)}';",
                    ct);
            }
            await ExecuteOnServerAsync(connection, "FLUSH PRIVILEGES;", ct);
            return true;
        }
        catch (MySqlException ex)
        {
            TestContext.Progress.WriteLine($"Limited user setup failed: {ex.Message}");
            return false;
        }
    }

    private static async Task DropUserIfExistsAsync(ConnectionConfig connection, string username, CancellationToken ct)
    {
        try
        {
            foreach (var host in LimitedUserHosts)
            {
                await ExecuteOnServerAsync(
                    connection,
                    $"DROP USER IF EXISTS '{EscapeSqlString(username)}'@'{EscapeSqlString(host)}';",
                    ct);
            }
            await ExecuteOnServerAsync(connection, "FLUSH PRIVILEGES;", ct);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Limited user cleanup failed: {ex.Message}");
        }
    }

    private static readonly string[] LimitedUserHosts = ["%", "localhost", "127.0.0.1"];

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
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE @prefix;", conn);
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

    private static async Task ExecuteOnServerAsync(ConnectionConfig connection, string sql, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildServerConnectionString(connection));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteOnDatabaseAsync(
        ConnectionConfig connection, string dbName, string sql, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static ConnectionConfig CopyConnection(ConnectionConfig source, string username, string password) =>
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
}
