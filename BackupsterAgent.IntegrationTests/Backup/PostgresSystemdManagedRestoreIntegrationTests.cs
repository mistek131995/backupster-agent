using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace BackupsterAgent.IntegrationTests.Backup;

[TestFixture]
[Category("Integration")]
[Category("PostgresSystemd")]
[NonParallelizable]
public sealed class PostgresSystemdManagedRestoreIntegrationTests
{
    private const string TestDbPrefix = "bp_itest_pg_systemd_";
    private const string InvalidConfigLine = "backupster_invalid_setting_for_start_failure = on";

    private ConnectionConfig _baseConnection = null!;
    private ExternalProcessRunner _runner = null!;
    private PostgresBinaryResolver _resolver = null!;
    private string _initDbBinary = null!;
    private string _postgresBinary = null!;
    private string _pgIsReadyBinary = null!;
    private string _serviceUser = null!;
    private string _serviceGroup = null!;
    private bool _runCommandsAsServiceUser;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Assume.That(OperatingSystem.IsLinux(), Is.True,
            "PostgreSQL systemd-managed restore integration tests require Linux.");
        Assume.That(Directory.Exists("/run/systemd/system"), Is.True,
            "System systemd runtime directory '/run/systemd/system' is not available.");
        Assume.That(CanWriteSystemdRuntimeDirectory(), Is.True,
            "PostgreSQL systemd-managed restore integration tests require permission to create temporary systemd units in /run/systemd/system.");

        Assume.That(
            IntegrationConfig.TryGetPostgresConnection(out var connection),
            Is.True,
            "Postgres:* not configured; set via dotnet user-secrets or BACKUPSTER_INTEGRATION_POSTGRES__* env vars.");

        _baseConnection = connection;
        _runner = new ExternalProcessRunner(NullLogger<ExternalProcessRunner>.Instance);
        _resolver = new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance);

        using var bootCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        _initDbBinary = await _resolver.ResolveAsync(_baseConnection, "initdb", bootCts.Token);
        _postgresBinary = await _resolver.ResolveAsync(_baseConnection, "postgres", bootCts.Token);
        _pgIsReadyBinary = await _resolver.ResolveAsync(_baseConnection, "pg_isready", bootCts.Token);
        Assume.That(Path.IsPathFullyQualified(_initDbBinary), Is.True,
            "PostgreSQL systemd tests require initdb to resolve to an absolute path; set Postgres:BinPath if needed.");
        Assume.That(Path.IsPathFullyQualified(_postgresBinary), Is.True,
            "PostgreSQL systemd tests require postgres to resolve to an absolute path; set Postgres:BinPath if needed.");
        Assume.That(Path.IsPathFullyQualified(_pgIsReadyBinary), Is.True,
            "PostgreSQL systemd tests require pg_isready to resolve to an absolute path; set Postgres:BinPath if needed.");

        var uid = (await RunProcessAsync("id", ["-u"], TimeSpan.FromSeconds(10), bootCts.Token)).Stdout.Trim();
        if (uid == "0")
        {
            Assume.That(
                (await RunProcessAsync("id", ["-u", "postgres"], TimeSpan.FromSeconds(10), bootCts.Token)).ExitCode,
                Is.Zero,
                "Root-run PostgreSQL systemd tests require an OS user named 'postgres'.");
            _serviceUser = "postgres";
            _runCommandsAsServiceUser = true;
        }
        else
        {
            var currentUser = (await RunProcessAsync("id", ["-un"], TimeSpan.FromSeconds(10), bootCts.Token)).Stdout.Trim();
            Assume.That(currentUser, Is.Not.Empty, "Could not resolve current OS user.");
            _serviceUser = currentUser;
            _runCommandsAsServiceUser = false;
        }

        var groupResult = await RunProcessAsync("id", ["-gn", _serviceUser], TimeSpan.FromSeconds(10), bootCts.Token);
        Assume.That(groupResult.ExitCode, Is.Zero, $"Could not resolve primary group for OS user '{_serviceUser}'.");
        _serviceGroup = groupResult.Stdout.Trim();

        await AssumeSystemctlAvailableAsync(bootCts.Token);
    }

    [SetUp]
    public void SetUp()
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
    }

    [TearDown]
    public void TearDown()
    {
        _cts?.Dispose();
    }

    [Test]
    public async Task RestoreProvider_SystemdManagedCluster_RoundTrip_RestoresAndRestartsService()
    {
        await using var cluster = await CreateManagedClusterAsync(wrapperMainPid: false, _cts.Token);
        var database = TestDbPrefix + Guid.NewGuid().ToString("N")[..8];
        var outputDir = Path.Combine(cluster.RootDir, "out");
        Directory.CreateDirectory(outputDir);

        await CreateDatabaseWithRowsAsync(cluster.Connection, database, InitialRows, _cts.Token);

        var backupProvider = BuildBackupProvider();
        var restoreProvider = BuildRestoreProvider();
        var backup = await backupProvider.BackupAsync(MakeConfig(database, outputDir), cluster.Connection, _cts.Token);

        await InsertRowsAsync(cluster.Connection, database, PostBackupRows, _cts.Token);

        await restoreProvider.ValidatePermissionsAsync(cluster.Connection, database, _cts.Token);
        await restoreProvider.ValidateRestoreSourceAsync(cluster.Connection, backup.FilePath, _cts.Token);
        await restoreProvider.RestoreAsync(cluster.Connection, database, database, backup.FilePath, _cts.Token);

        await WaitForServerReadyAsync(cluster.Connection, _cts.Token);
        var rows = await ReadRowsAsync(cluster.Connection, database, _cts.Token);
        var unitActive = await IsUnitActiveAsync(cluster.UnitName, _cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EquivalentTo(InitialRows));
            Assert.That(unitActive, Is.True);
            Assert.That(Directory.Exists(cluster.PgData), Is.True);
            Assert.That(FindSiblingDirs(cluster.PgData, "old"), Is.Empty);
            Assert.That(FindSiblingDirs(cluster.PgData, "new"), Is.Empty);
            Assert.That(FindSiblingDirs(cluster.PgData, "failed"), Is.Empty);
        });
    }

    [Test]
    public async Task ValidatePermissions_SystemdMainPidMismatch_FailsBeforeSwap()
    {
        await using var cluster = await CreateManagedClusterAsync(wrapperMainPid: true, _cts.Token);
        var database = TestDbPrefix + Guid.NewGuid().ToString("N")[..8];
        await CreateDatabaseWithRowsAsync(cluster.Connection, database, InitialRows, _cts.Token);

        var restoreProvider = BuildRestoreProvider();

        var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
            restoreProvider.ValidatePermissionsAsync(cluster.Connection, database, _cts.Token));

        var unitActive = await IsUnitActiveAsync(cluster.UnitName, _cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("MainPID"));
            Assert.That(ex.Message, Does.Contain("postmaster.pid"));
            Assert.That(unitActive, Is.True);
            Assert.That(Directory.Exists(cluster.PgData), Is.True);
            Assert.That(FindSiblingDirs(cluster.PgData, "old"), Is.Empty);
            Assert.That(FindSiblingDirs(cluster.PgData, "new"), Is.Empty);
        });
    }

    [Test]
    public async Task RestoreProvider_SystemdStartFailure_RollsBackOriginalCluster()
    {
        await using var cluster = await CreateManagedClusterAsync(wrapperMainPid: false, _cts.Token);
        var database = TestDbPrefix + Guid.NewGuid().ToString("N")[..8];
        var outputDir = Path.Combine(cluster.RootDir, "out");
        Directory.CreateDirectory(outputDir);

        await CreateDatabaseWithRowsAsync(cluster.Connection, database, InitialRows, _cts.Token);

        AppendInvalidConfigLine(cluster.PgData);
        var backup = await BuildBackupProvider().BackupAsync(MakeConfig(database, outputDir), cluster.Connection, _cts.Token);
        RemoveInvalidConfigLine(cluster.PgData);

        await InsertRowsAsync(cluster.Connection, database, PostBackupRows, _cts.Token);

        var restoreProvider = BuildRestoreProvider();
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            restoreProvider.RestoreAsync(cluster.Connection, database, database, backup.FilePath, _cts.Token));

        await WaitForServerReadyAsync(cluster.Connection, _cts.Token);
        var rows = await ReadRowsAsync(cluster.Connection, database, _cts.Token);
        var unitActive = await IsUnitActiveAsync(cluster.UnitName, _cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.Null);
            Assert.That(rows, Is.EquivalentTo(InitialRows.Concat(PostBackupRows)));
            Assert.That(unitActive, Is.True);
            Assert.That(Directory.Exists(cluster.PgData), Is.True);
            Assert.That(FindSiblingDirs(cluster.PgData, "old"), Is.Empty);
            Assert.That(FindSiblingDirs(cluster.PgData, "failed"), Is.Not.Empty);
        });
    }

    private PostgresPhysicalBackupProvider BuildBackupProvider() =>
        new(
            NullLogger<PostgresPhysicalBackupProvider>.Instance,
            _resolver,
            _runner);

    private PostgresPhysicalRestoreProvider BuildRestoreProvider()
    {
        var restoreSettings = Options.Create(new RestoreSettings
        {
            PgCtlStartTimeoutSeconds = 45,
            SystemctlTimeoutSeconds = 20,
            SystemctlStopStartTimeoutSeconds = 60,
            ChownTimeoutSeconds = 60,
        });
        var lifecycle = new PostgresClusterLifecycle(
            NullLogger<PostgresClusterLifecycle>.Instance,
            _runner,
            restoreSettings);

        return new PostgresPhysicalRestoreProvider(
            NullLogger<PostgresPhysicalRestoreProvider>.Instance,
            _resolver,
            restoreSettings,
            lifecycle);
    }

    private DatabaseConfig MakeConfig(string database, string outputDir) => new()
    {
        ConnectionName = "pg-systemd-itest",
        StorageName = "n/a",
        Database = database,
        OutputPath = outputDir,
    };

    private async Task<ManagedPostgresCluster> CreateManagedClusterAsync(bool wrapperMainPid, CancellationToken ct)
    {
        var root = Path.Combine(Path.GetTempPath(), "backupster-pg-systemd-" + Guid.NewGuid().ToString("N"));
        var pgData = Path.Combine(root, "pgdata");
        var port = FindFreeLoopbackPort();
        var unitName = "backupster-pg-itest-" + Guid.NewGuid().ToString("N")[..12] + ".service";
        var unitPath = Path.Combine("/run/systemd/system", unitName);
        var wrapperPath = Path.Combine(root, "postgres-wrapper.sh");

        Directory.CreateDirectory(root);
        await ChownIfNeededAsync(root, ct);

        await RunAsServiceUserAsync(
            _initDbBinary,
            ["-D", pgData, "-A", "trust", "-U", "postgres", "--encoding=UTF8", "--no-locale"],
            TimeSpan.FromSeconds(120),
            ct);

        AppendPostgresConfOverrides(pgData, port);
        EnsureLocalTrustHbaFile(pgData);

        if (wrapperMainPid)
        {
            await File.WriteAllTextAsync(
                wrapperPath,
                "#!/bin/sh\n" +
                $"{ShellQuote(_postgresBinary)} -D {ShellQuote(pgData)} &\n" +
                "wait $!\n",
                ct);
            if (OperatingSystem.IsLinux())
            {
                File.SetUnixFileMode(wrapperPath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                    UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                    UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
            }
            await ChownIfNeededAsync(wrapperPath, ct);
        }

        await File.WriteAllTextAsync(
            unitPath,
            BuildUnitFile(unitName, pgData, port, wrapperMainPid ? wrapperPath : null),
            ct);

        var daemonReload = await RunProcessAsync("systemctl", ["daemon-reload"], TimeSpan.FromSeconds(30), ct);
        Assume.That(daemonReload.ExitCode, Is.Zero,
            $"systemctl daemon-reload failed: {daemonReload.Stderr}{daemonReload.Stdout}");

        var cluster = new ManagedPostgresCluster(this, root, pgData, unitName, unitPath, port, new ConnectionConfig
        {
            Name = "pg-systemd-itest",
            DatabaseType = _baseConnection.DatabaseType,
            Host = "localhost",
            Port = port,
            Username = "postgres",
            Password = string.Empty,
            BinPath = _baseConnection.BinPath,
        });

        try
        {
            await StartUnitAsync(unitName, ct);
            await WaitForServerReadyAsync(cluster.Connection, ct);
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    private string BuildUnitFile(string unitName, string pgData, int port, string? wrapperPath)
    {
        var execStart = wrapperPath is null
            ? $"{_postgresBinary} -D {pgData}"
            : wrapperPath;

        return string.Join(Environment.NewLine,
        [
            "[Unit]",
            $"Description=Backupster PostgreSQL systemd integration test {unitName}",
            "",
            "[Service]",
            "Type=simple",
            $"User={_serviceUser}",
            $"Group={_serviceGroup}",
            "KillMode=mixed",
            "Restart=no",
            "TimeoutStartSec=60",
            "TimeoutStopSec=60",
            "Environment=LC_MESSAGES=C",
            "Environment=LANG=C",
            $"ExecStart={execStart}",
            "",
        ]);
    }

    private async Task StartUnitAsync(string unitName, CancellationToken ct)
    {
        var result = await RunProcessAsync("systemctl", ["start", unitName], TimeSpan.FromSeconds(90), ct);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"systemctl start {unitName} failed with exit {result.ExitCode}: {result.Stderr}{result.Stdout}");
    }

    private async Task StopAndRemoveUnitAsync(string unitName, string unitPath)
    {
        await TryRunCleanupProcessAsync("systemctl", ["stop", unitName], TimeSpan.FromSeconds(90));
        await TryRunCleanupProcessAsync("systemctl", ["reset-failed", unitName], TimeSpan.FromSeconds(20));

        try { if (File.Exists(unitPath)) File.Delete(unitPath); }
        catch (Exception ex) { TestContext.Progress.WriteLine($"Unit file cleanup failed for '{unitPath}': {ex.Message}"); }

        await TryRunCleanupProcessAsync("systemctl", ["daemon-reload"], TimeSpan.FromSeconds(30));
    }

    private async Task AssumeSystemctlAvailableAsync(CancellationToken ct)
    {
        var result = await RunProcessAsync("systemctl", ["--version"], TimeSpan.FromSeconds(10), ct);
        Assume.That(result.ExitCode, Is.Zero, $"systemctl is not available: {result.Stderr}{result.Stdout}");
    }

    private static bool CanWriteSystemdRuntimeDirectory()
    {
        var probe = Path.Combine("/run/systemd/system", "backupster-write-probe-" + Guid.NewGuid().ToString("N"));
        try
        {
            File.WriteAllText(probe, string.Empty);
            File.Delete(probe);
            return true;
        }
        catch
        {
            try { if (File.Exists(probe)) File.Delete(probe); } catch { }
            return false;
        }
    }

    private async Task RunAsServiceUserAsync(
        string fileName,
        IReadOnlyList<string> args,
        TimeSpan timeout,
        CancellationToken ct)
    {
        ProcessResult result;
        if (_runCommandsAsServiceUser)
        {
            var runUserArgs = new List<string> { "-u", _serviceUser, "--", fileName };
            runUserArgs.AddRange(args);
            result = await RunProcessAsync("runuser", runUserArgs, timeout, ct);
        }
        else
        {
            result = await RunProcessAsync(fileName, args, timeout, ct);
        }

        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"{fileName} failed with exit {result.ExitCode}: {result.Stderr}{result.Stdout}");
    }

    private async Task ChownIfNeededAsync(string path, CancellationToken ct)
    {
        if (!_runCommandsAsServiceUser) return;

        var result = await RunProcessAsync(
            "chown",
            ["-R", $"{_serviceUser}:{_serviceGroup}", path],
            TimeSpan.FromSeconds(30),
            ct);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"chown failed for '{path}' with exit {result.ExitCode}: {result.Stderr}{result.Stdout}");
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
        IReadOnlyList<string> args,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var psi = new System.Diagnostics.ProcessStartInfo
        {
            FileName = fileName,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = System.Text.Encoding.UTF8,
            StandardErrorEncoding = System.Text.Encoding.UTF8,
        };
        foreach (var arg in args)
            psi.ArgumentList.Add(arg);

        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";

        using var process = new System.Diagnostics.Process { StartInfo = psi };
        process.Start();

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        var stdoutTask = process.StandardOutput.ReadToEndAsync(timeoutCts.Token);
        var stderrTask = process.StandardError.ReadToEndAsync(timeoutCts.Token);

        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var stdout = await DrainAsync(stdoutTask);
            var stderr = await DrainAsync(stderrTask);
            return new ProcessResult(process.ExitCode, stdout, stderr);
        }
        catch (OperationCanceledException)
        {
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            catch { }
            throw;
        }
    }

    private static async Task<string> DrainAsync(Task<string> task)
    {
        try { return await task; }
        catch { return string.Empty; }
    }

    private static async Task TryRunCleanupProcessAsync(
        string fileName,
        IReadOnlyList<string> args,
        TimeSpan timeout)
    {
        var command = FormatCommand(fileName, args);
        try
        {
            var result = await RunProcessAsync(fileName, args, timeout, CancellationToken.None);
            if (result.ExitCode != 0)
                TestContext.Progress.WriteLine(
                    $"Cleanup command '{command}' failed with exit {result.ExitCode}: {result.Stderr}{result.Stdout}");
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Cleanup command '{command}' failed: {ex.Message}");
        }
    }

    private static string FormatCommand(string fileName, IReadOnlyList<string> args) =>
        args.Count == 0 ? fileName : fileName + " " + string.Join(" ", args);

    private async Task WaitForServerReadyAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        Exception? last = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection));
                await conn.OpenAsync(ct);
                return;
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException)
            {
                last = ex;
                await Task.Delay(500, ct);
            }
        }

        throw new TimeoutException($"PostgreSQL on port {connection.Port} did not become ready.", last);
    }

    private async Task<bool> IsUnitActiveAsync(string unitName, CancellationToken ct)
    {
        var result = await RunProcessAsync("systemctl", ["is-active", unitName], TimeSpan.FromSeconds(10), ct);
        return result.ExitCode == 0 && result.Stdout.Trim() == "active";
    }

    private static async Task CreateDatabaseWithRowsAsync(
        ConnectionConfig connection,
        string database,
        IEnumerable<(int Id, string Name)> rows,
        CancellationToken ct)
    {
        await using (var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection)))
        {
            await conn.OpenAsync(ct);
            await using var create = new NpgsqlCommand($"CREATE DATABASE \"{database}\";", conn);
            await create.ExecuteNonQueryAsync(ct);
        }

        await InsertRowsAsync(connection, database, rows, ct);
    }

    private static async Task InsertRowsAsync(
        ConnectionConfig connection,
        string database,
        IEnumerable<(int Id, string Name)> rows,
        CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildDatabaseConnectionString(connection, database));
        await conn.OpenAsync(ct);

        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, name text NOT NULL);", conn))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        foreach (var (id, name) in rows)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO items (id, name) VALUES (@id, @name) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;", conn);
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("name", name);
            await insert.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<(int Id, string Name)>> ReadRowsAsync(
        ConnectionConfig connection,
        string database,
        CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildDatabaseConnectionString(connection, database));
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand("SELECT id, name FROM items ORDER BY id;", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        var rows = new List<(int Id, string Name)>();
        while (await reader.ReadAsync(ct))
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    private static void AppendPostgresConfOverrides(string pgData, int port)
    {
        var confPath = Path.Combine(pgData, "postgresql.conf");
        File.AppendAllText(confPath, string.Join(Environment.NewLine,
        [
            string.Empty,
            "# overrides appended by Backupster systemd integration test",
            $"port = {port}",
            "listen_addresses = 'localhost'",
            "unix_socket_directories = ''",
            "ssl = off",
            string.Empty,
        ]));
    }

    private static void EnsureLocalTrustHbaFile(string pgData)
    {
        File.WriteAllText(Path.Combine(pgData, "pg_hba.conf"), string.Join(Environment.NewLine,
        [
            "local all all trust",
            "host all all 127.0.0.1/32 trust",
            "host all all ::1/128 trust",
            "host replication all 127.0.0.1/32 trust",
            "host replication all ::1/128 trust",
            string.Empty,
        ]));
    }

    private static void AppendInvalidConfigLine(string pgData)
    {
        File.AppendAllText(Path.Combine(pgData, "postgresql.conf"),
            Environment.NewLine + InvalidConfigLine + Environment.NewLine);
    }

    private static void RemoveInvalidConfigLine(string pgData)
    {
        var confPath = Path.Combine(pgData, "postgresql.conf");
        var lines = File.ReadAllLines(confPath)
            .Where(line => !string.Equals(line.Trim(), InvalidConfigLine, StringComparison.Ordinal))
            .ToArray();
        File.WriteAllLines(confPath, lines);
    }

    private static string[] FindSiblingDirs(string pgData, string suffix)
    {
        var parent = Path.GetDirectoryName(pgData)!;
        var leaf = Path.GetFileName(pgData.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        return Directory.Exists(parent)
            ? Directory.GetDirectories(parent, $"{leaf}.{suffix}-*")
            : [];
    }

    private static int FindFreeLoopbackPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try { return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private static string ShellQuote(string value) => "'" + value.Replace("'", "'\"'\"'") + "'";

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
            catch (ArgumentException) { }
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"PID kill failed for '{pgData}': {ex.Message}");
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Directory cleanup failed for '{path}': {ex.Message}");
        }
    }

    private static readonly (int Id, string Name)[] InitialRows =
    [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ];

    private static readonly (int Id, string Name)[] PostBackupRows =
    [
        (4, "delta"),
        (5, "epsilon"),
    ];

    private sealed record ProcessResult(int ExitCode, string Stdout, string Stderr);

    private sealed class ManagedPostgresCluster(
        PostgresSystemdManagedRestoreIntegrationTests owner,
        string rootDir,
        string pgData,
        string unitName,
        string unitPath,
        int port,
        ConnectionConfig connection) : IAsyncDisposable
    {
        public string RootDir { get; } = rootDir;
        public string PgData { get; } = pgData;
        public string UnitName { get; } = unitName;
        public string UnitPath { get; } = unitPath;
        public int Port { get; } = port;
        public ConnectionConfig Connection { get; } = connection;

        public async ValueTask DisposeAsync()
        {
            try
            {
                await owner.StopAndRemoveUnitAsync(UnitName, UnitPath);
            }
            finally
            {
                TryKillByPidFile(PgData);
                TryDeleteDirectory(RootDir);
            }
        }
    }
}
