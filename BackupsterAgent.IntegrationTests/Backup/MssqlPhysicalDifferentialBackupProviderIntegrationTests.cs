using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
public sealed class MssqlPhysicalDifferentialBackupProviderIntegrationTests
{
    private const string TestDbPrefix = "bp_itest_diff_mssql_";

    private ConnectionConfig _connection = null!;
    private string _outputPath = null!;
    private string _srcDb = null!;
    private string _dstDb = null!;
    private DateTime _testStartUtc;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(
            IntegrationConfig.TryGetMssqlConnection(out var connection),
            Is.True,
            "Mssql:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_MSSQL__* env vars.");

        _connection = connection;
        Assume.That(
            IntegrationConfig.TryGetMssqlOutputPath(out _outputPath),
            Is.True,
            "Mssql:OutputPath is required for MSSQL physical integration tests. It must be visible and writable to both the agent process and SQL Server.");
        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await DropLeftoverTestDatabasesAsync(_connection, bootCts.Token);
    }

    [SetUp]
    public async Task SetUp()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _srcDb = TestDbPrefix + "src_" + suffix;
        _dstDb = TestDbPrefix + "dst_" + suffix;

        _testStartUtc = DateTime.UtcNow;
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));

        await CreateSourceDatabaseAsync(_connection, _srcDb, _cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        try { await DropDatabaseIfExistsAsync(_connection, _srcDb, CancellationToken.None); }
        catch (Exception ex) { TestContext.Progress.WriteLine($"Source DB cleanup failed: {ex.Message}"); }

        try { await DropDatabaseIfExistsAsync(_connection, _dstDb, CancellationToken.None); }
        catch (Exception ex) { TestContext.Progress.WriteLine($"Target DB cleanup failed: {ex.Message}"); }

        _cts?.Dispose();
    }

    [Test]
    public async Task FullThenDiff_DiffBakIsCreated()
    {
        var fullProvider = new MssqlPhysicalBackupProvider(NullLogger<MssqlPhysicalBackupProvider>.Instance);
        var diffProvider = new MssqlPhysicalDifferentialBackupProvider(
            NullLogger<MssqlPhysicalDifferentialBackupProvider>.Instance,
            fullProvider,
            new MssqlDifferentialChainGuard());

        var config = MakeConfig();

        var fullResult = await fullProvider.BackupAsync(config, _connection, _cts.Token);
        try
        {
            Assert.That(fullResult.Success, Is.True);
            Assert.That(File.Exists(fullResult.FilePath), Is.True);

            await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);

            var diffCtx = new DifferentialBackupContext
            {
                BaseBackupRecordId = Guid.NewGuid(),
                BaseDumpObjectKey = BuildDumpObjectKey(fullResult.FilePath),
            };
            var diffResult = await diffProvider.BackupAsync(config, _connection, diffCtx, _cts.Token);
            try
            {
                Assert.That(diffResult.Success, Is.True);
                Assert.That(File.Exists(diffResult.FilePath), Is.True, "diff bak file must exist after backup");
                Assert.That(diffResult.SizeBytes, Is.GreaterThan(0));
                Assert.That(
                    Path.GetFileName(diffResult.FilePath),
                    Does.Contain("_diff.bak"),
                    "diff backup file name must carry the _diff.bak suffix");

                Assert.That(
                    Path.GetFullPath(Path.GetDirectoryName(diffResult.FilePath)!).TrimEnd('\\', '/'),
                    Is.EqualTo(Path.GetFullPath(_outputPath).TrimEnd('\\', '/')).IgnoreCase,
                    "Diff provider must write the bak under DatabaseConfig.OutputPath");
                Assert.That(
                    File.GetLastWriteTimeUtc(diffResult.FilePath),
                    Is.GreaterThanOrEqualTo(_testStartUtc.AddSeconds(-2)),
                    "Returned FilePath must point to a file produced by this run");
            }
            finally
            {
                await TryDeleteBakFileAsync(_connection, diffResult.FilePath);
            }
        }
        finally
        {
            await TryDeleteBakFileAsync(_connection, fullResult.FilePath);
        }
    }

    [Test]
    public async Task RoundTrip_RestoreFullThenDiff_HasAllRows()
    {
        var fullProvider = new MssqlPhysicalBackupProvider(NullLogger<MssqlPhysicalBackupProvider>.Instance);
        var diffProvider = new MssqlPhysicalDifferentialBackupProvider(
            NullLogger<MssqlPhysicalDifferentialBackupProvider>.Instance,
            fullProvider,
            new MssqlDifferentialChainGuard());
        var fullRestoreProvider = new MssqlPhysicalRestoreProvider(
            NullLogger<MssqlPhysicalRestoreProvider>.Instance);
        var diffRestoreProvider = new MssqlPhysicalDifferentialRestoreProvider(
            NullLogger<MssqlPhysicalDifferentialRestoreProvider>.Instance,
            fullRestoreProvider);

        var config = MakeConfig();

        var fullResult = await fullProvider.BackupAsync(config, _connection, _cts.Token);
        try
        {
            await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);

            var diffCtx = new DifferentialBackupContext
            {
                BaseBackupRecordId = Guid.NewGuid(),
                BaseDumpObjectKey = BuildDumpObjectKey(fullResult.FilePath),
            };
            var diffResult = await diffProvider.BackupAsync(config, _connection, diffCtx, _cts.Token);
            try
            {
                var fullSqlPath = fullResult.FilePath;
                var diffSqlPath = diffResult.FilePath;

                var chain = new[]
                {
                    new DifferentialRestoreChainItem
                    {
                        BackupRecordId = Guid.NewGuid(),
                        DumpFilePath = fullSqlPath,
                        BackupMode = BackupMode.Physical,
                    },
                    new DifferentialRestoreChainItem
                    {
                        BackupRecordId = Guid.NewGuid(),
                        DumpFilePath = diffSqlPath,
                        BackupMode = BackupMode.PhysicalDifferential,
                    },
                };

                await diffRestoreProvider.ValidatePermissionsAsync(_connection, _dstDb, _cts.Token);
                await diffRestoreProvider.ValidateRestoreSourceAsync(_connection, chain, _cts.Token);
                await diffRestoreProvider.PrepareTargetDatabaseAsync(_connection, _dstDb, _cts.Token);
                await diffRestoreProvider.RestoreAsync(_connection, _dstDb, chain, _cts.Token);

                var restoredRows = await ReadItemsAsync(_connection, _dstDb, _cts.Token);

                Assert.That(restoredRows, Is.EquivalentTo(InitialRows.Concat(PostFullRows)));
            }
            finally
            {
                await TryDeleteBakFileAsync(_connection, diffResult.FilePath);
            }
        }
        finally
        {
            await TryDeleteBakFileAsync(_connection, fullResult.FilePath);
        }
    }

    [Test]
    public async Task DiffAfterRestoreToOlderDiff_WithNewerFullAsBase_ThrowsChainBroken()
    {
        var fullProvider = new MssqlPhysicalBackupProvider(NullLogger<MssqlPhysicalBackupProvider>.Instance);
        var diffProvider = new MssqlPhysicalDifferentialBackupProvider(
            NullLogger<MssqlPhysicalDifferentialBackupProvider>.Instance,
            fullProvider,
            new MssqlDifferentialChainGuard());
        var fullRestoreProvider = new MssqlPhysicalRestoreProvider(
            NullLogger<MssqlPhysicalRestoreProvider>.Instance);
        var diffRestoreProvider = new MssqlPhysicalDifferentialRestoreProvider(
            NullLogger<MssqlPhysicalDifferentialRestoreProvider>.Instance,
            fullRestoreProvider);

        var config = MakeConfig();

        var full1Result = await fullProvider.BackupAsync(config, _connection, _cts.Token);
        try
        {
            await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);

            var diff1Result = await diffProvider.BackupAsync(
                config,
                _connection,
                new DifferentialBackupContext
                {
                    BaseBackupRecordId = Guid.NewGuid(),
                    BaseDumpObjectKey = BuildDumpObjectKey(full1Result.FilePath),
                },
                _cts.Token);
            try
            {
                await InsertRowsAsync(_connection, _srcDb, PostDiffRows, _cts.Token);

                var full2Result = await fullProvider.BackupAsync(config, _connection, _cts.Token);
                try
                {
                    var oldChain = new[]
                    {
                        new DifferentialRestoreChainItem
                        {
                            BackupRecordId = Guid.NewGuid(),
                            DumpFilePath = full1Result.FilePath,
                            BackupMode = BackupMode.Physical,
                        },
                        new DifferentialRestoreChainItem
                        {
                            BackupRecordId = Guid.NewGuid(),
                            DumpFilePath = diff1Result.FilePath,
                            BackupMode = BackupMode.PhysicalDifferential,
                        },
                    };

                    await diffRestoreProvider.ValidatePermissionsAsync(_connection, _srcDb, _cts.Token);
                    await diffRestoreProvider.ValidateRestoreSourceAsync(_connection, oldChain, _cts.Token);
                    await diffRestoreProvider.PrepareTargetDatabaseAsync(_connection, _srcDb, _cts.Token);
                    await diffRestoreProvider.RestoreAsync(_connection, _srcDb, oldChain, _cts.Token);

                    var ex = await TryRunDivergedDiffAsync(
                        diffProvider,
                        config,
                        full2Result.FilePath,
                        _cts.Token);

                    Assert.That(ex, Is.Not.Null);
                    Assert.That(ex, Is.TypeOf<DifferentialChainBrokenException>());
                }
                finally
                {
                    await TryDeleteBakFileAsync(_connection, full2Result.FilePath);
                }
            }
            finally
            {
                await TryDeleteBakFileAsync(_connection, diff1Result.FilePath);
            }
        }
        finally
        {
            await TryDeleteBakFileAsync(_connection, full1Result.FilePath);
        }
    }

    [Test]
    public void DiffWithoutPriorFull_Throws()
    {
        var fullProvider = new MssqlPhysicalBackupProvider(NullLogger<MssqlPhysicalBackupProvider>.Instance);
        var diffProvider = new MssqlPhysicalDifferentialBackupProvider(
            NullLogger<MssqlPhysicalDifferentialBackupProvider>.Instance,
            fullProvider,
            new MssqlDifferentialChainGuard());

        var config = MakeConfig();
        var ctx = new DifferentialBackupContext
        {
            BaseBackupRecordId = Guid.NewGuid(),
        };

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => diffProvider.BackupAsync(config, _connection, ctx, _cts.Token));

        Assert.That(
            ex!.Message,
            Does.Contain("полного бэкапа").IgnoreCase,
            "Error message must explain that no prior full backup exists");
    }

    private DatabaseConfig MakeConfig() => new()
    {
        ConnectionName = _connection.Name,
        StorageName = "n/a",
        Database = _srcDb,
        OutputPath = _outputPath,
    };

    private static readonly (int Id, string Name)[] InitialRows =
    [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ];

    private static readonly (int Id, string Name)[] PostFullRows =
    [
        (4, "delta"),
        (5, "epsilon"),
    ];

    private static readonly (int Id, string Name)[] PostDiffRows =
    [
        (6, "zeta"),
        (7, "eta"),
    ];

    private async Task<Exception?> TryRunDivergedDiffAsync(
        MssqlPhysicalDifferentialBackupProvider diffProvider,
        DatabaseConfig config,
        string full2Path,
        CancellationToken ct)
    {
        try
        {
            var unexpectedResult = await diffProvider.BackupAsync(
                config,
                _connection,
                new DifferentialBackupContext
                {
                    BaseBackupRecordId = Guid.NewGuid(),
                    BaseDumpObjectKey = BuildDumpObjectKey(full2Path),
                },
                ct);

            await TryDeleteBakFileAsync(_connection, unexpectedResult.FilePath);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static async Task CreateSourceDatabaseAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        await ExecuteOnMasterAsync(connection, $"CREATE DATABASE [{Escape(dbName)}];", ct);

        const string ddl = @"
CREATE TABLE dbo.Items (
    Id   INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL
);";
        await ExecuteOnDatabaseAsync(connection, dbName, ddl, ct);
        await InsertRowsAsync(connection, dbName, InitialRows, ct);
    }

    private static async Task InsertRowsAsync(
        ConnectionConfig connection, string dbName, (int Id, string Name)[] rows, CancellationToken ct)
    {
        foreach (var (id, name) in rows)
        {
            await using var conn = new SqlConnection(BuildConnectionString(connection, dbName));
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand("INSERT INTO dbo.Items (Id, Name) VALUES (@id, @name);", conn);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", name);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<(int Id, string Name)>> ReadItemsAsync(
        ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        var rows = new List<(int, string)>();
        await using var conn = new SqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT Id, Name FROM dbo.Items;", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    private static async Task DropDatabaseIfExistsAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        var sql = $@"
IF DB_ID(N'{EscapeForString(dbName)}') IS NOT NULL
BEGIN
    ALTER DATABASE [{Escape(dbName)}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{Escape(dbName)}];
END";
        await ExecuteOnMasterAsync(connection, sql, ct);
    }

    private static async Task DropLeftoverTestDatabasesAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var leftovers = new List<string>();
        await using (var conn = new SqlConnection(BuildMasterConnectionString(connection)))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(
                $"SELECT name FROM sys.databases WHERE name LIKE N'{TestDbPrefix}%';", conn);
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

    private static async Task TryDeleteBakFileAsync(ConnectionConfig connection, string agentPath)
    {
        if (string.IsNullOrWhiteSpace(agentPath)) return;

        try { if (File.Exists(agentPath)) File.Delete(agentPath); }
        catch (Exception ex) { TestContext.Progress.WriteLine($"Local bak delete '{agentPath}' failed: {ex.Message}"); }

        using var cleanupCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await using var conn = new SqlConnection(BuildMasterConnectionString(connection));
            await conn.OpenAsync(cleanupCts.Token);
            await using var cmd = new SqlCommand(
                $"EXEC master.sys.xp_delete_files N'{EscapeForString(agentPath)}';", conn)
            { CommandTimeout = 30 };
            await cmd.ExecuteNonQueryAsync(cleanupCts.Token);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"SQL-side bak delete '{agentPath}' failed: {ex.Message}");
        }
    }

    private static async Task ExecuteOnMasterAsync(ConnectionConfig connection, string sql, CancellationToken ct)
    {
        await using var conn = new SqlConnection(BuildMasterConnectionString(connection));
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 300 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteOnDatabaseAsync(
        ConnectionConfig connection, string dbName, string sql, CancellationToken ct)
    {
        await using var conn = new SqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string BuildDumpObjectKey(string bakPath) =>
        $"integration/{Path.GetFileName(bakPath)}.enc";

    private static string BuildMasterConnectionString(ConnectionConfig connection) =>
        BuildConnectionString(connection, "master");

    private static string BuildConnectionString(ConnectionConfig connection, string database) =>
        new SqlConnectionStringBuilder
        {
            DataSource = $"{connection.Host},{connection.Port}",
            InitialCatalog = database,
            UserID = connection.Username,
            Password = connection.Password,
            Encrypt = true,
            TrustServerCertificate = true,
        }.ConnectionString;

    private static string Escape(string identifier) => identifier.Replace("]", "]]");
    private static string EscapeForString(string s) => s.Replace("'", "''");
}
