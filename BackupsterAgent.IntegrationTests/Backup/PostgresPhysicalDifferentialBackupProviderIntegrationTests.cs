using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class PostgresPhysicalDifferentialBackupProviderIntegrationTests
{
    private const string TestDbPrefix = "bp_itest_pg_diff_";
    private const int MinimumMajorVersion = 17;

    private ConnectionConfig _connection = null!;
    private ExternalProcessRunner _runner = null!;
    private PostgresBinaryResolver _resolver = null!;
    private string _pgCtlBinary = null!;
    private string _pgCombineBinary = null!;
    private string _initDbBinary = null!;
    private int _majorVersion;
    private string? _originalSummarizeWal;
    private bool _summarizeWalChanged;

    private string _srcDb = null!;
    private string _fullOutputDir = null!;
    private string _diffOutputDir = null!;
    private string _restoreDir = null!;
    private string _serverLogPath = null!;
    private int _restorePort;
    private bool _serverStarted;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(
            IntegrationConfig.TryGetPostgresConnection(out var connection),
            Is.True,
            "Postgres:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_POSTGRES__* env vars.");

        _connection = connection;
        _runner = new ExternalProcessRunner(NullLogger<ExternalProcessRunner>.Instance);
        _resolver = new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance);

        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        _majorVersion = await _resolver.GetMajorVersionAsync(_connection, bootCts.Token);
        Assume.That(
            _majorVersion,
            Is.GreaterThanOrEqualTo(MinimumMajorVersion),
            $"Differential backups require PostgreSQL {MinimumMajorVersion}+, but cluster is {_majorVersion}.");

        _originalSummarizeWal = await GetSettingAsync(_connection, "summarize_wal", bootCts.Token);
        if (!string.Equals(_originalSummarizeWal, "on", StringComparison.OrdinalIgnoreCase))
        {
            await SetSummarizeWalAsync(_connection, "on", bootCts.Token);
            _summarizeWalChanged = true;
            // Give the WAL summarizer process some time to start summarizing recent WAL activity
            // before any test runs — otherwise the very first --incremental might be missing summaries.
            await Task.Delay(TimeSpan.FromSeconds(3), bootCts.Token);
        }

        await DropLeftoverTestDatabasesAsync(_connection, bootCts.Token);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (!_summarizeWalChanged) return;

        using var cleanupCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await SetSummarizeWalAsync(_connection, _originalSummarizeWal ?? "off", cleanupCts.Token);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Failed to restore summarize_wal to '{_originalSummarizeWal}': {ex.Message}");
        }
    }

    [SetUp]
    public async Task SetUp()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _srcDb = TestDbPrefix + "src_" + suffix;

        _fullOutputDir = Path.Combine(Path.GetTempPath(), "backupster-pg-diff-full-" + Guid.NewGuid().ToString("N"));
        _diffOutputDir = Path.Combine(Path.GetTempPath(), "backupster-pg-diff-diff-" + Guid.NewGuid().ToString("N"));
        _restoreDir = Path.Combine(Path.GetTempPath(), "backupster-pg-diff-restore-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_fullOutputDir);
        Directory.CreateDirectory(_diffOutputDir);

        _serverLogPath = Path.Combine(_fullOutputDir, "test-server.log");
        _serverStarted = false;

        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));

        _pgCtlBinary = await _resolver.ResolveAsync(_connection, "pg_ctl", _cts.Token);
        _pgCombineBinary = await _resolver.ResolveAsync(_connection, "pg_combinebackup", _cts.Token);
        _initDbBinary = await _resolver.ResolveAsync(_connection, "initdb", _cts.Token);

        await CreateSourceDatabaseAsync(_connection, _srcDb, _cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_serverStarted)
        {
            await TryStopRestoreServerAsync();
            _serverStarted = false;
        }

        try { await DropDatabaseIfExistsAsync(_connection, _srcDb, CancellationToken.None); }
        catch (Exception ex) { TestContext.Progress.WriteLine($"Source DB cleanup failed: {ex.Message}"); }

        _cts?.Dispose();

        TryDeleteDirectory(_restoreDir, "restore dir");
        TryDeleteDirectory(_fullOutputDir, "full output dir");
        TryDeleteDirectory(_diffOutputDir, "diff output dir");
    }

    [Test]
    public async Task FullThenDiff_DiffArchiveAndManifestAreCreated()
    {
        var (fullProvider, diffProvider) = BuildProviders();

        var fullResult = await fullProvider.BackupAsync(MakeConfig(_fullOutputDir), _connection, _cts.Token);

        Assert.That(fullResult.Success, Is.True);
        Assert.That(File.Exists(fullResult.FilePath), Is.True);
        Assert.That(fullResult.PgBaseManifestPath, Is.Not.Null,
            "FULL provider must capture backup_manifest for downstream incremental backups");
        Assert.That(File.Exists(fullResult.PgBaseManifestPath!), Is.True);

        await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);
        await ExecuteOnDatabaseAsync(_connection, _srcDb, "CHECKPOINT;", _cts.Token);
        await WaitForWalSummarizerAsync(_connection, _cts.Token);

        var ctx = new DifferentialBackupContext
        {
            BaseBackupRecordId = Guid.NewGuid(),
            BasePgBaseManifestPath = fullResult.PgBaseManifestPath,
        };

        var diffResult = await diffProvider.BackupAsync(
            MakeConfig(_diffOutputDir), _connection, ctx, _cts.Token);

        Assert.That(diffResult.Success, Is.True);
        Assert.That(File.Exists(diffResult.FilePath), Is.True);
        Assert.That(Path.GetFileName(diffResult.FilePath), Does.Contain("_diff.tar.gz"));
        Assert.That(diffResult.PgBaseManifestPath, Is.Not.Null,
            "DIFF provider must capture a fresh backup_manifest for next-tier incremental");
        Assert.That(File.Exists(diffResult.PgBaseManifestPath!), Is.True);
    }

    [Test]
    public async Task RoundTrip_RestoredCombinedClusterHasAllRows()
    {
        var (fullProvider, diffProvider) = BuildProviders();

        var fullResult = await fullProvider.BackupAsync(MakeConfig(_fullOutputDir), _connection, _cts.Token);
        Assert.That(fullResult.PgBaseManifestPath, Is.Not.Null);

        await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);
        await ExecuteOnDatabaseAsync(_connection, _srcDb, "CHECKPOINT;", _cts.Token);
        await WaitForWalSummarizerAsync(_connection, _cts.Token);

        var ctx = new DifferentialBackupContext
        {
            BaseBackupRecordId = Guid.NewGuid(),
            BasePgBaseManifestPath = fullResult.PgBaseManifestPath,
        };
        var diffResult = await diffProvider.BackupAsync(
            MakeConfig(_diffOutputDir), _connection, ctx, _cts.Token);

        var fullStaging = Path.Combine(_fullOutputDir, "staging-full");
        var diffStaging = Path.Combine(_diffOutputDir, "staging-diff");
        Directory.CreateDirectory(fullStaging);
        Directory.CreateDirectory(diffStaging);

        await ExtractTarGzAsync(fullResult.FilePath, fullStaging, _cts.Token);
        await ExtractTarGzAsync(diffResult.FilePath, diffStaging, _cts.Token);

        var combineExit = await RunPgCombineBackupAsync(fullStaging, diffStaging, _restoreDir, _cts.Token);
        Assert.That(combineExit, Is.EqualTo(0), "pg_combinebackup must succeed");
        Assert.That(File.Exists(Path.Combine(_restoreDir, "PG_VERSION")), Is.True,
            "combined cluster must have PG_VERSION");

        await StartRestoreServerWithPortRetryAsync(maxAttempts: 3);
        await WaitForServerReadyAsync(_restorePort, _connection, _cts.Token);

        var rows = await ReadItemsAsync(_connection, _restorePort, _srcDb, _cts.Token);
        Assert.That(rows, Is.EquivalentTo(InitialRows.Concat(PostFullRows)));
    }

    [Test]
    public async Task RoundTrip_RestoreProviderRestoresChainToSeparateCluster()
    {
        var (fullProvider, diffProvider) = BuildProviders();
        var (fullRestoreProvider, diffRestoreProvider) = BuildRestoreProviders();

        var fullResult = await fullProvider.BackupAsync(MakeConfig(_fullOutputDir), _connection, _cts.Token);
        Assert.That(fullResult.PgBaseManifestPath, Is.Not.Null);

        await InsertRowsAsync(_connection, _srcDb, PostFullRows, _cts.Token);
        await ExecuteOnDatabaseAsync(_connection, _srcDb, "CHECKPOINT;", _cts.Token);
        await WaitForWalSummarizerAsync(_connection, _cts.Token);

        var ctx = new DifferentialBackupContext
        {
            BaseBackupRecordId = Guid.NewGuid(),
            BasePgBaseManifestPath = fullResult.PgBaseManifestPath,
        };
        var diffResult = await diffProvider.BackupAsync(
            MakeConfig(_diffOutputDir), _connection, ctx, _cts.Token);

        var sourcePgData = await QuerySourcePgDataAsync(_connection, _cts.Token);
        var targetPgData = Path.Combine(
            Path.GetTempPath(),
            "backupster-pg-diff-target-" + Guid.NewGuid().ToString("N"));
        var targetServerLog = targetPgData + ".log";
        var sourcePort = _connection.Port;
        var targetPort = FindFreeLoopbackPort();
        var sourceWasRunning = false;
        var combinedClusterStarted = false;

        try
        {
            await RunInitDbAsync(_initDbBinary, targetPgData, _cts.Token);
            AppendPostgresConfOverrides(targetPgData, targetPort);

            await StartClusterAsync(targetPgData, targetServerLog, _cts.Token);
            try
            {
                await WaitForServerReadyOnPortAsync("postgres", targetPort, _cts.Token);
            }
            catch
            {
                await TryStopClusterAsync(targetPgData);
                throw;
            }

            sourceWasRunning = true;
            await StopClusterFastAsync(sourcePgData, _cts.Token);

            var chain = new[]
            {
                new DifferentialRestoreChainItem
                {
                    BackupRecordId = Guid.NewGuid(),
                    DumpFilePath = fullResult.FilePath,
                    BackupMode = BackupMode.Physical,
                },
                new DifferentialRestoreChainItem
                {
                    BackupRecordId = Guid.NewGuid(),
                    DumpFilePath = diffResult.FilePath,
                    BackupMode = BackupMode.PhysicalDifferential,
                },
            };

            var targetConnection = new ConnectionConfig
            {
                Name = "pg-target-itest",
                DatabaseType = _connection.DatabaseType,
                Host = "localhost",
                Port = targetPort,
                Username = "postgres",
                Password = string.Empty,
                BinPath = _connection.BinPath,
            };

            await diffRestoreProvider.ValidatePermissionsAsync(targetConnection, _srcDb, _cts.Token);
            await diffRestoreProvider.ValidateRestoreSourceAsync(targetConnection, chain, _cts.Token);
            await diffRestoreProvider.PrepareTargetDatabaseAsync(targetConnection, _srcDb, _cts.Token);
            await diffRestoreProvider.RestoreAsync(targetConnection, _srcDb, chain, _cts.Token);

            combinedClusterStarted = true;
            await WaitForServerReadyAsync(sourcePort, _connection, _cts.Token);

            var rows = await ReadItemsAsync(_connection, sourcePort, _srcDb, _cts.Token);
            Assert.That(rows, Is.EquivalentTo(InitialRows.Concat(PostFullRows)));

            var combineWorkDir = Directory.GetParent(targetPgData)!.EnumerateDirectories(
                Path.GetFileName(targetPgData) + ".combine").FirstOrDefault();
            Assert.That(combineWorkDir, Is.Null,
                "combineWorkDir must be cleaned up after successful restore");

            Assert.That(File.Exists(Path.Combine(targetPgData, "PG_VERSION")), Is.True,
                "swapped staging must be in place at targetPgData");
        }
        finally
        {
            if (combinedClusterStarted)
                await TryStopClusterAsync(targetPgData);

            TryDeleteDirectory(targetPgData, "target pgdata");
            TryDeleteFile(targetServerLog, "target server log");
            TryCleanupSiblings(targetPgData);

            if (sourceWasRunning)
            {
                try
                {
                    await StartClusterAsync(sourcePgData, sourcePgData + ".restart.log", _cts.Token);
                    await WaitForServerReadyAsync(sourcePort, _connection, _cts.Token);
                }
                catch (Exception ex)
                {
                    TestContext.Progress.WriteLine($"Failed to restart source cluster: {ex.Message}");
                }
                finally
                {
                    TryDeleteFile(sourcePgData + ".restart.log", "source restart log");
                }
            }
        }
    }

    [Test]
    public void DiffWithoutBaseManifest_Throws()
    {
        var (_, diffProvider) = BuildProviders();

        var ctx = new DifferentialBackupContext
        {
            BaseBackupRecordId = Guid.NewGuid(),
            BasePgBaseManifestPath = null,
        };

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => diffProvider.BackupAsync(MakeConfig(_diffOutputDir), _connection, ctx, _cts.Token));

        Assert.That(
            ex!.Message,
            Does.Contain("backup_manifest").IgnoreCase,
            "Error message must explain the missing backup_manifest input");
    }

    private (PostgresPhysicalRestoreProvider Full, PostgresPhysicalDifferentialRestoreProvider Diff) BuildRestoreProviders()
    {
        var restoreSettings = Microsoft.Extensions.Options.Options.Create(new RestoreSettings());
        var full = new PostgresPhysicalRestoreProvider(
            NullLogger<PostgresPhysicalRestoreProvider>.Instance, _resolver, restoreSettings);
        var diff = new PostgresPhysicalDifferentialRestoreProvider(
            NullLogger<PostgresPhysicalDifferentialRestoreProvider>.Instance, full, _resolver, _runner);
        return (full, diff);
    }

    private async Task RunInitDbAsync(string initdbBinary, string pgDataPath, CancellationToken ct)
    {
        var psi = new System.Diagnostics.ProcessStartInfo
        {
            FileName = initdbBinary,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("-D");
        psi.ArgumentList.Add(pgDataPath);
        psi.ArgumentList.Add("-A");
        psi.ArgumentList.Add("trust");
        psi.ArgumentList.Add("-U");
        psi.ArgumentList.Add("postgres");
        psi.ArgumentList.Add("--encoding=UTF8");
        psi.ArgumentList.Add("--no-locale");
        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";

        using var process = new System.Diagnostics.Process { StartInfo = psi };
        process.Start();
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"initdb at '{pgDataPath}' failed with exit code {process.ExitCode}");
    }

    private async Task StartClusterAsync(string pgDataPath, string serverLog, CancellationToken ct)
    {
        var exitCode = await RunPgCtlDirectAsync(
            new[] { "-D", pgDataPath, "-l", serverLog, "-w", "-t", "120", "start" },
            timeout: TimeSpan.FromSeconds(150),
            ct);
        if (exitCode != 0)
        {
            var tail = TryReadServerLogTail(serverLog, 80);
            throw new InvalidOperationException(
                $"pg_ctl start failed for '{pgDataPath}' with exit {exitCode}. Server log tail:{Environment.NewLine}{tail}");
        }
    }

    private async Task StopClusterFastAsync(string pgDataPath, CancellationToken ct)
    {
        var exitCode = await RunPgCtlDirectAsync(
            new[] { "-D", pgDataPath, "-m", "fast", "-w", "-t", "60", "stop" },
            timeout: TimeSpan.FromSeconds(75),
            ct);
        if (exitCode != 0)
            throw new InvalidOperationException(
                $"pg_ctl stop failed for '{pgDataPath}' with exit {exitCode}");
    }

    private async Task TryStopClusterAsync(string pgDataPath)
    {
        using var cleanupCts = new CancellationTokenSource(TimeSpan.FromSeconds(75));
        try
        {
            await StopClusterFastAsync(pgDataPath, cleanupCts.Token);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Best-effort stop of '{pgDataPath}' failed: {ex.Message}");
            try
            {
                await RunPgCtlDirectAsync(
                    new[] { "-D", pgDataPath, "-m", "immediate", "-w", "-t", "30", "stop" },
                    timeout: TimeSpan.FromSeconds(40),
                    cleanupCts.Token);
            }
            catch (Exception ex2)
            {
                TestContext.Progress.WriteLine($"Immediate stop also failed: {ex2.Message}");
                TryKillByPidFile(pgDataPath);
            }
        }
    }

    private static async Task<string> QuerySourcePgDataAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("SHOW data_directory;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result is not string s || string.IsNullOrWhiteSpace(s))
            throw new InvalidOperationException("SHOW data_directory returned empty value on source cluster.");
        return s;
    }

    private static async Task WaitForServerReadyOnPortAsync(string username, int port, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var connString = new NpgsqlConnectionStringBuilder
        {
            Host = "localhost",
            Port = port,
            Username = username,
            Database = "postgres",
            Timeout = 2,
        }.ToString();

        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await using var conn = new NpgsqlConnection(connString);
                await conn.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand("SELECT 1;", conn);
                await cmd.ExecuteScalarAsync(ct);
                return;
            }
            catch (Exception ex) when (IsTransientStartupError(ex))
            {
                last = ex;
                await Task.Delay(500, ct);
            }
        }

        throw new TimeoutException(
            $"Cluster on port {port} did not become ready within 30 seconds. Last error: {last?.Message}");
    }

    private static void TryCleanupSiblings(string pgDataPath)
    {
        var parent = Path.GetDirectoryName(pgDataPath);
        var leaf = Path.GetFileName(pgDataPath);
        if (parent is null || string.IsNullOrEmpty(leaf)) return;

        foreach (var pattern in new[] { $"{leaf}.new-*", $"{leaf}.old-*", $"{leaf}.failed-*" })
        {
            try
            {
                foreach (var dir in Directory.EnumerateDirectories(parent, pattern))
                {
                    try { Directory.Delete(dir, recursive: true); }
                    catch (Exception ex)
                    {
                        TestContext.Progress.WriteLine($"Failed to delete leftover '{dir}': {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                TestContext.Progress.WriteLine($"Sibling enumeration failed for '{pattern}': {ex.Message}");
            }
        }
    }

    private static void TryDeleteFile(string path, string description)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"{description} cleanup failed for '{path}': {ex.Message}");
        }
    }

    private (PostgresPhysicalBackupProvider Full, PostgresPhysicalDifferentialBackupProvider Diff) BuildProviders()
    {
        var full = new PostgresPhysicalBackupProvider(
            NullLogger<PostgresPhysicalBackupProvider>.Instance, _resolver, _runner);
        var diff = new PostgresPhysicalDifferentialBackupProvider(
            NullLogger<PostgresPhysicalDifferentialBackupProvider>.Instance, _resolver, _runner, full);
        return (full, diff);
    }

    private DatabaseConfig MakeConfig(string outputDir) => new()
    {
        ConnectionName = _connection.Name,
        StorageName = "n/a",
        Database = _srcDb,
        OutputPath = outputDir,
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
        (6, "zeta"),
    ];

    private async Task<int> RunPgCombineBackupAsync(
        string fullStaging, string diffStaging, string outputDir, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = _pgCombineBinary,
            Arguments = new[] { "-o", outputDir, fullStaging, diffStaging },
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        var result = await _runner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
        if (result.ExitCode != 0)
        {
            TestContext.Progress.WriteLine($"pg_combinebackup stderr: {result.Stderr}");
        }
        return result.ExitCode;
    }

    private async Task StartRestoreServerWithPortRetryAsync(int maxAttempts)
    {
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            _restorePort = FindFreeLoopbackPort();
            AppendPostgresConfOverrides(_restoreDir, _restorePort);

            var exitCode = await RunPgCtlDirectAsync(
                new[] { "-D", _restoreDir, "-l", _serverLogPath, "-w", "-t", "120", "start" },
                timeout: TimeSpan.FromSeconds(150),
                _cts.Token);

            if (exitCode == 0)
            {
                _serverStarted = true;
                return;
            }

            var tail = TryReadServerLogTail(_serverLogPath, 60);
            if (attempt < maxAttempts && LooksLikePortBindFailure(tail))
            {
                TestContext.Progress.WriteLine(
                    $"pg_ctl start (attempt {attempt}) failed on port {_restorePort}; retrying with a fresh port.");
                continue;
            }

            throw new InvalidOperationException(
                $"pg_ctl start failed with exit {exitCode} on attempt {attempt}/{maxAttempts}. Server log tail:{Environment.NewLine}{tail}");
        }
    }

    private static bool LooksLikePortBindFailure(string serverLogTail) =>
        serverLogTail.Contains("could not bind", StringComparison.OrdinalIgnoreCase)
        || serverLogTail.Contains("Address already in use", StringComparison.OrdinalIgnoreCase)
        || serverLogTail.Contains("could not create any TCP/IP sockets", StringComparison.OrdinalIgnoreCase);

    private async Task TryStopRestoreServerAsync()
    {
        try
        {
            var fast = await RunPgCtlStopDirectAsync("fast", timeoutSeconds: 10);
            if (fast == 0) return;
            var immediate = await RunPgCtlStopDirectAsync("immediate", timeoutSeconds: 10);
            if (immediate != 0)
            {
                TryKillByPidFile(_restoreDir);
            }
        }
        catch
        {
            TryKillByPidFile(_restoreDir);
        }
    }

    private async Task<int> RunPgCtlStopDirectAsync(string mode, int timeoutSeconds)
    {
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds + 10));
        return await RunPgCtlDirectAsync(
            new[] { "-D", _restoreDir, "-m", mode, "-w", "-t", timeoutSeconds.ToString(), "stop" },
            timeout: TimeSpan.FromSeconds(timeoutSeconds + 10),
            stopCts.Token);
    }

    private async Task<int> RunPgCtlDirectAsync(string[] args, TimeSpan timeout, CancellationToken ct)
    {
        var psi = new System.Diagnostics.ProcessStartInfo
        {
            FileName = _pgCtlBinary,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var a in args) psi.ArgumentList.Add(a);
        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";

        using var process = new System.Diagnostics.Process { StartInfo = psi };
        process.Start();

        using var combined = CancellationTokenSource.CreateLinkedTokenSource(ct);
        combined.CancelAfter(timeout);

        try
        {
            await process.WaitForExitAsync(combined.Token);
        }
        catch (OperationCanceledException)
        {
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            catch { /* best-effort */ }
            throw;
        }

        return process.ExitCode;
    }

    private static void TryKillByPidFile(string pgData)
    {
        try
        {
            var pidFile = Path.Combine(pgData, "postmaster.pid");
            if (!File.Exists(pidFile)) return;
            var firstLine = File.ReadLines(pidFile).FirstOrDefault();
            if (!int.TryParse(firstLine, out var pid)) return;
            try
            {
                using var proc = System.Diagnostics.Process.GetProcessById(pid);
                proc.Kill(entireProcessTree: true);
            }
            catch (ArgumentException) { /* already gone */ }
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"PID kill failed: {ex.Message}");
        }
    }

    private static async Task ExtractTarGzAsync(string archivePath, string targetDir, CancellationToken ct)
    {
        Directory.CreateDirectory(targetDir);
        await using var fileStream = File.OpenRead(archivePath);
        await using var gz = new GZipStream(fileStream, CompressionMode.Decompress);
        await TarFile.ExtractToDirectoryAsync(gz, targetDir, overwriteFiles: true, ct);
    }

    private static int FindFreeLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private static void AppendPostgresConfOverrides(string pgData, int port)
    {
        var confPath = Path.Combine(pgData, "postgresql.conf");
        var overrides = string.Join(Environment.NewLine,
        [
            string.Empty,
            "# overrides appended by integration test",
            $"port = {port}",
            "listen_addresses = 'localhost'",
            "unix_socket_directories = ''",
            "ssl = off",
            string.Empty,
        ]);
        File.AppendAllText(confPath, overrides);
    }

    private static async Task WaitForServerReadyAsync(int port, ConnectionConfig connection, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var connString = new NpgsqlConnectionStringBuilder
        {
            Host = "localhost",
            Port = port,
            Username = connection.Username,
            Password = connection.Password,
            Database = "postgres",
            Timeout = 2,
        }.ToString();

        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await using var conn = new NpgsqlConnection(connString);
                await conn.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand("SELECT 1;", conn);
                await cmd.ExecuteScalarAsync(ct);
                return;
            }
            catch (Exception ex) when (IsTransientStartupError(ex))
            {
                last = ex;
                await Task.Delay(500, ct);
            }
        }

        throw new TimeoutException(
            $"Restored server on port {port} did not become ready within 30 seconds. Last error: {last?.Message}");
    }

    private static bool IsTransientStartupError(Exception ex)
    {
        switch (ex)
        {
            case SocketException:
            case TimeoutException:
                return true;
            case PostgresException pg:
                return pg.SqlState == "57P03";
            case NpgsqlException npg when npg.InnerException is SocketException:
                return true;
            default:
                return false;
        }
    }

    private static async Task WaitForWalSummarizerAsync(ConnectionConfig connection, CancellationToken ct)
    {
        // Push current WAL to a known LSN, then poll until the summarizer has caught up.
        // Without this wait, the very next `pg_basebackup --incremental` may fail with
        // "WAL summaries are required" because summarization is async.
        var connString = BuildConnectionString(connection, "postgres");

        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);

        string targetLsn;
        await using (var cmd = new NpgsqlCommand("SELECT pg_current_wal_lsn()::text;", conn))
        {
            targetLsn = (string)(await cmd.ExecuteScalarAsync(ct))!;
        }

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            await using var cmd = new NpgsqlCommand(
                "SELECT COALESCE(MAX(end_lsn), '0/0'::pg_lsn) >= @lsn::pg_lsn FROM pg_available_wal_summaries();", conn);
            cmd.Parameters.AddWithValue("lsn", targetLsn);
            var caught = (bool?)await cmd.ExecuteScalarAsync(ct);
            if (caught == true) return;

            await Task.Delay(500, ct);
        }

        TestContext.Progress.WriteLine(
            $"WAL summarizer did not reach LSN {targetLsn} within 30s; incremental may fail.");
    }

    private static async Task CreateSourceDatabaseAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        await ExecuteOnDatabaseAsync(connection, "postgres", $"CREATE DATABASE \"{dbName}\";", ct);

        var ddl = "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL);";
        await ExecuteOnDatabaseAsync(connection, dbName, ddl, ct);

        await InsertRowsAsync(connection, dbName, InitialRows, ct);
        await ExecuteOnDatabaseAsync(connection, dbName, "CHECKPOINT;", ct);
    }

    private static async Task InsertRowsAsync(
        ConnectionConfig connection, string dbName, (int Id, string Name)[] rows, CancellationToken ct)
    {
        foreach (var (id, name) in rows)
        {
            await using var conn = new NpgsqlConnection(BuildConnectionString(connection, dbName));
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand("INSERT INTO items (id, name) VALUES (@id, @name);", conn);
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("name", name);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<(int Id, string Name)>> ReadItemsAsync(
        ConnectionConfig connection, int port, string dbName, CancellationToken ct)
    {
        var connString = new NpgsqlConnectionStringBuilder
        {
            Host = "localhost",
            Port = port,
            Username = connection.Username,
            Password = connection.Password,
            Database = dbName,
            Timeout = 5,
        }.ToString();

        var rows = new List<(int, string)>();
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT id, name FROM items;", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    private static async Task<string?> GetSettingAsync(ConnectionConfig connection, string name, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"SHOW {name};", conn);
        return (string?)await cmd.ExecuteScalarAsync(ct);
    }

    private static async Task SetSummarizeWalAsync(ConnectionConfig connection, string value, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
        await conn.OpenAsync(ct);
        await using (var cmd = new NpgsqlCommand($"ALTER SYSTEM SET summarize_wal = '{value}';", conn))
            await cmd.ExecuteNonQueryAsync(ct);
        await using (var cmd = new NpgsqlCommand("SELECT pg_reload_conf();", conn))
            await cmd.ExecuteScalarAsync(ct);
    }

    private static async Task DropDatabaseIfExistsAsync(ConnectionConfig connection, string dbName, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
        await conn.OpenAsync(ct);

        await using (var terminate = new NpgsqlCommand(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
            "WHERE datname = @db AND pid <> pg_backend_pid();", conn))
        {
            terminate.Parameters.AddWithValue("db", dbName);
            await terminate.ExecuteNonQueryAsync(ct);
        }

        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{dbName}\";", conn);
        await drop.ExecuteNonQueryAsync(ct);
    }

    private static async Task DropLeftoverTestDatabasesAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var leftovers = new List<string>();
        await using (var conn = new NpgsqlConnection(BuildConnectionString(connection, "postgres")))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                "SELECT datname FROM pg_database WHERE datname LIKE @prefix;", conn);
            cmd.Parameters.AddWithValue("prefix", TestDbPrefix + "%");
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

    private static async Task ExecuteOnDatabaseAsync(
        ConnectionConfig connection, string dbName, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(BuildConnectionString(connection, dbName));
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string BuildConnectionString(ConnectionConfig connection, string database) =>
        new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Port = connection.Port,
            Username = connection.Username,
            Password = connection.Password,
            Database = database,
        }.ToString();

    private static string TryReadServerLogTail(string path, int lineCount)
    {
        try
        {
            if (!File.Exists(path)) return "(server log not found)";
            var lines = File.ReadAllLines(path);
            var tail = lines.Length <= lineCount ? lines : lines[^lineCount..];
            return string.Join(Environment.NewLine, tail);
        }
        catch (Exception ex)
        {
            return $"(failed to read server log: {ex.Message})";
        }
    }

    private static void TryDeleteDirectory(string path, string description)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"{description} cleanup failed for '{path}': {ex.Message}");
        }
    }
}
