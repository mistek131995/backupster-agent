using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

public sealed class RestoreCommonTests
{
    private string _workDir = null!;

    [SetUp]
    public void SetUp()
    {
        _workDir = Path.Combine(Path.GetTempPath(), $"backupster-restore-common-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_workDir);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (Directory.Exists(_workDir))
                Directory.Delete(_workDir, recursive: true);
        }
        catch
        {
        }
    }

    [Test]
    public void TryParseSystemdUnit_MultipleServices_PrefersMatchingService()
    {
        var cgroup = "0::/system.slice/docker.service/system.slice/mysql.service";

        var unit = SystemdUnitDetector.TryParseSystemdUnit(
            cgroup,
            serviceName => serviceName.Contains("mysql", StringComparison.OrdinalIgnoreCase));

        Assert.That(unit, Is.EqualTo("mysql.service"));
    }

    [Test]
    public void TryParseSystemdUnit_MultiplePreferredServices_ReturnsNull()
    {
        var cgroup = "0::/system.slice/mysql.service/mariadb.service";

        var unit = SystemdUnitDetector.TryParseSystemdUnit(
            cgroup,
            serviceName => serviceName.Contains("mysql", StringComparison.OrdinalIgnoreCase) ||
                           serviceName.Contains("mariadb", StringComparison.OrdinalIgnoreCase));

        Assert.That(unit, Is.Null);
    }

    [Test]
    public void SplitPath_RootPath_Throws()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            RestorePathResolver.SplitPath(root, "data directory"));

        Assert.That(ex!.Message, Does.Contain("data directory"));
    }

    [Test]
    public void CleanupOrphanStagingDirs_DeletesOldMarkedDirectoryOnlyForConfiguredSuffixes()
    {
        var markerStore = new RestoreMarkerStore(NullLogger<RestoreMarkerStore>.Instance);
        var oldNew = Path.Combine(_workDir, "data.new-old");
        var oldFailed = Path.Combine(_workDir, "data.failed-old");
        var oldOld = Path.Combine(_workDir, "data.old-old");
        var freshNew = Path.Combine(_workDir, "data.new-fresh");

        Directory.CreateDirectory(oldNew);
        Directory.CreateDirectory(oldFailed);
        Directory.CreateDirectory(oldOld);
        Directory.CreateDirectory(freshNew);

        WriteMarker(oldNew, DateTime.UtcNow.AddHours(-72));
        WriteMarker(oldFailed, DateTime.UtcNow.AddHours(-72));
        WriteMarker(oldOld, DateTime.UtcNow.AddHours(-72));
        WriteMarker(freshNew, DateTime.UtcNow);

        markerStore.CleanupOrphanStagingDirs(
            _workDir, "data", ["new", "failed"], TimeSpan.FromHours(48));

        Assert.That(Directory.Exists(oldNew), Is.False);
        Assert.That(Directory.Exists(oldFailed), Is.False);
        Assert.That(Directory.Exists(oldOld), Is.True);
        Assert.That(Directory.Exists(freshNew), Is.True);
    }

    [Test]
    public void EnsureSameFsRename_NormalDirectory_Succeeds()
    {
        var preflight = new FilesystemRenamePreflight(NullLogger<FilesystemRenamePreflight>.Instance);
        var live = Path.Combine(_workDir, "data");
        Directory.CreateDirectory(live);

        Assert.DoesNotThrow(() =>
            preflight.EnsureSameFsRename(_workDir, live, "data directory", throwRestorePermissionException: true));
    }

    [Test]
    public void EnsureMainPid_Mismatch_ThrowsPermissionException()
    {
        var runner = new MainPidRunner("999");
        var controller = new SystemdServiceController(
            NullLogger<SystemdServiceController>.Instance,
            runner,
            Options.Create(new RestoreSettings()));

        var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
            controller.EnsureMainPidAsync("mysql.service", 123, "MySQL-сервис", "mysqld", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("MainPID=999"));
    }

    [Test]
    public void IsActiveAsync_Canceled_PropagatesCancellation()
    {
        var runner = new CanceledRunner();
        var controller = new SystemdServiceController(
            NullLogger<SystemdServiceController>.Instance,
            runner,
            Options.Create(new RestoreSettings()));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() =>
            controller.IsActiveAsync("postgresql.service", cts.Token));
    }

    private static void WriteMarker(string dir, DateTime timestamp)
    {
        File.WriteAllText(
            Path.Combine(dir, RestoreMarkerStore.MarkerFileName),
            timestamp.ToString("o"));
    }

    private sealed class MainPidRunner : IExternalProcessRunner
    {
        private readonly string _mainPid;

        public MainPidRunner(string mainPid)
        {
            _mainPid = mainPid;
        }

        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct) =>
            Task.FromResult(new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = _mainPid,
                Stderr = string.Empty,
            });
    }

    private sealed class CanceledRunner : IExternalProcessRunner
    {
        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            });
        }
    }
}
