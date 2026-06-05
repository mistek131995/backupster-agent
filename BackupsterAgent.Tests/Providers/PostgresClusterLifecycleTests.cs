using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

public sealed class PostgresClusterLifecycleTests
{
    [Test]
    public void TryParseSystemdUnit_NestedPostgresSlice_ReturnsClusterUnit()
    {
        var cgroup = "0::/system.slice/system-postgresql.slice/postgresql@16-main.service";

        var unit = PostgresClusterLifecycle.TryParseSystemdUnit(cgroup);

        Assert.That(unit, Is.EqualTo("postgresql@16-main.service"));
    }

    [Test]
    public void TryParseSystemdUnit_MultipleServicesPrefersPostgres()
    {
        var cgroup = "0::/system.slice/docker.service/postgresql@17-prod.service";

        var unit = PostgresClusterLifecycle.TryParseSystemdUnit(cgroup);

        Assert.That(unit, Is.EqualTo("postgresql@17-prod.service"));
    }

    [Test]
    public void TryParseSystemdUnit_NoService_ReturnsNull()
    {
        var unit = PostgresClusterLifecycle.TryParseSystemdUnit("0::/user.slice/user-1000.slice/session-2.scope");

        Assert.That(unit, Is.Null);
    }

    [Test]
    public void Stop_Systemd_UsesSystemctlStop()
    {
        var runner = new RecordingPostgresProcessRunner();
        var lifecycle = CreateLifecycle(runner);
        var control = new PostgresClusterControl(
            PostgresClusterControlKind.Systemd, "postgresql@16-main.service", null, null);

        Assert.DoesNotThrowAsync(() =>
            lifecycle.StopAsync(control, "pg_ctl", "/var/lib/postgresql/16/main", CancellationToken.None));

        Assert.That(runner.Requests.Select(r => $"{r.FileName} {string.Join(" ", r.Arguments)}"),
            Does.Contain("systemctl stop postgresql@16-main.service"));
    }

    [Test]
    public void Stop_Unmanaged_UsesPgCtlFastStop()
    {
        var runner = new RecordingPostgresProcessRunner();
        var lifecycle = CreateLifecycle(runner);
        var control = new PostgresClusterControl(PostgresClusterControlKind.Unmanaged, null, null, null);

        Assert.DoesNotThrowAsync(() =>
            lifecycle.StopAsync(control, "pg_ctl", "/pgdata", CancellationToken.None));

        var request = runner.Requests.Single();
        Assert.That(request.FileName, Is.EqualTo("pg_ctl"));
        Assert.That(request.Arguments, Is.EqualTo(new[] { "stop", "-D", "/pgdata", "-m", "fast", "-w" }));
    }

    [Test]
    public void Start_SystemdFailure_IncludesJournalDiagnostics()
    {
        var runner = new RecordingPostgresProcessRunner
        {
            ExitCodes = { ["systemctl start"] = 1 },
            Stdout = { ["journalctl"] = "postgres failed to start" },
        };
        var lifecycle = CreateLifecycle(runner);
        var control = new PostgresClusterControl(
            PostgresClusterControlKind.Systemd, "postgresql@16-main.service", null, null);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            lifecycle.StartAsync(control, "pg_ctl", "/pgdata", "start.log", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("postgresql@16-main.service"));
        Assert.That(ex.Message, Does.Contain("postgres failed to start"));
    }

    [Test]
    public void Start_WindowsService_UsesStartServiceScript()
    {
        var runner = new RecordingPostgresProcessRunner();
        var lifecycle = CreateLifecycle(runner);
        var control = new PostgresClusterControl(
            PostgresClusterControlKind.WindowsService, "postgresql-x64-16", "NT AUTHORITY\\NETWORK SERVICE", null);

        Assert.DoesNotThrowAsync(() =>
            lifecycle.StartAsync(control, "pg_ctl", @"C:\pgdata", "start.log", CancellationToken.None));

        var request = runner.Requests.Single();
        Assert.That(request.FileName, Is.EqualTo("powershell"));
        Assert.That(string.Join(" ", request.Arguments), Does.Contain("Start-Service -Name 'postgresql-x64-16'"));
        Assert.That(string.Join(" ", request.Arguments), Does.Contain("WaitForStatus('Running'"));
    }

    private static PostgresClusterLifecycle CreateLifecycle(RecordingPostgresProcessRunner runner)
    {
        var settings = Options.Create(new RestoreSettings
        {
            PgCtlStartTimeoutSeconds = 1,
            SystemctlTimeoutSeconds = 1,
            SystemctlStopStartTimeoutSeconds = 1,
        });

        return new PostgresClusterLifecycle(
            NullLogger<PostgresClusterLifecycle>.Instance, runner, settings);
    }

    private sealed class RecordingPostgresProcessRunner : IExternalProcessRunner
    {
        public List<ExternalProcessRequest> Requests { get; } = new();
        public Dictionary<string, int> ExitCodes { get; } = new();
        public Dictionary<string, string> Stdout { get; } = new();

        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            Requests.Add(request);

            var key = request.FileName == "systemctl" && request.Arguments.Count >= 2
                ? $"systemctl {request.Arguments[0]}"
                : request.FileName;
            var exitCode = ExitCodes.TryGetValue(key, out var code) ? code : 0;

            return Task.FromResult(new ExternalProcessResult
            {
                ExitCode = exitCode,
                Stdout = Stdout.TryGetValue(request.FileName, out var stdout) ? stdout : string.Empty,
                Stderr = exitCode == 0 ? string.Empty : $"{request.FileName} failed",
            });
        }
    }
}
