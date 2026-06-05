using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlDatadirSwapCoordinatorTests
{
    [Test]
    public void SwapAsync_StartFailsAfterReplacement_RestoresOldDatadirAndMovesNewAside()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        var lifecycle = new RecordingLifecycle();
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("new datadir start failed"));
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("new datadir start failed"));
        AssertMarker(layout.RealDatadir, "old");
        AssertMarker(layout.FailedPath, "new");
        Assert.That(Directory.Exists(layout.OldPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir, layout.RealDatadir }));
        Assert.That(lifecycle.StopCalls, Is.EqualTo(1));
    }

    [Test]
    public void SwapAsync_StagingMoveFailsAfterOldMove_RestoresOldDatadir()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        swapper.MoveFailures.Add(layout.StagingPath);
        var lifecycle = new RecordingLifecycle();
        var coordinator = CreateCoordinator(swapper, lifecycle);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        AssertMarker(layout.RealDatadir, "old");
        Assert.That(Directory.Exists(layout.OldPath), Is.False);
        Assert.That(Directory.Exists(layout.FailedPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir }));
        Assert.That(lifecycle.StopCalls, Is.EqualTo(1));
    }

    [Test]
    public void SwapAsync_FixOwnershipFailsAfterReplacement_RestoresOldDatadirAndMovesNewAside()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper
        {
            FixOwnershipException = new InvalidOperationException("chown failed"),
        };
        var lifecycle = new RecordingLifecycle();
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("chown failed"));
        AssertMarker(layout.RealDatadir, "old");
        AssertMarker(layout.FailedPath, "new");
        Assert.That(Directory.Exists(layout.OldPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir }));
    }

    [Test]
    public void SwapAsync_RecoveryCannotMoveNewDatadirAside_ReturnsManualRecoveryError()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        swapper.TryMoveFailures.Add(layout.RealDatadir);
        var lifecycle = new RecordingLifecycle();
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("new datadir start failed"));
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain(layout.RealDatadir));
        Assert.That(ex.Message, Does.Contain(layout.OldPath));
        AssertMarker(layout.RealDatadir, "new");
        AssertMarker(layout.OldPath, "old");
        Assert.That(Directory.Exists(layout.FailedPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir }));
    }

    [Test]
    public void SwapAsync_RecoveryCannotRestoreOldDatadir_ReturnsManualRecoveryError()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        swapper.TryMoveFailures.Add(layout.OldPath);
        var lifecycle = new RecordingLifecycle();
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("new datadir start failed"));
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain(layout.RealDatadir));
        Assert.That(ex.Message, Does.Contain(layout.OldPath));
        Assert.That(Directory.Exists(layout.RealDatadir), Is.False);
        AssertMarker(layout.OldPath, "old");
        AssertMarker(layout.FailedPath, "new");
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir }));
    }

    [Test]
    public void SwapAsync_RecoveryFindsBothDatadirsMissing_ReturnsDataLossError()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        var lifecycle = new RecordingLifecycle
        {
            OnStart = () =>
            {
                Directory.Delete(layout.RealDatadir, recursive: true);
                Directory.Delete(layout.OldPath, recursive: true);
                throw new InvalidOperationException("new datadir start failed");
            },
        };
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain(layout.RealDatadir));
        Assert.That(ex.Message, Does.Contain(layout.OldPath));
        Assert.That(Directory.Exists(layout.RealDatadir), Is.False);
        Assert.That(Directory.Exists(layout.OldPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir }));
    }

    [Test]
    public void SwapAsync_RecoveryRestoresDatadirButRestartFails_ReturnsManualStartError()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        var lifecycle = new RecordingLifecycle();
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("new datadir start failed"));
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("old datadir start failed"));
        var coordinator = CreateCoordinator(swapper, lifecycle);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("old datadir start failed"));
        AssertMarker(layout.RealDatadir, "old");
        AssertMarker(layout.FailedPath, "new");
        Assert.That(Directory.Exists(layout.OldPath), Is.False);
        Assert.That(lifecycle.StartDatadirs, Is.EqualTo(new[] { layout.RealDatadir, layout.RealDatadir }));
    }

    [Test]
    public void SwapAsync_ServiceManagedInstance_UnmasksServiceAfterFailure()
    {
        using var layout = SwapLayout.Create();
        var swapper = new RecordingSwapper();
        var lifecycle = new RecordingLifecycle();
        lifecycle.StartFailures.Enqueue(new InvalidOperationException("new datadir start failed"));
        var coordinator = CreateCoordinator(swapper, lifecycle);
        var instanceInfo = new MysqlInstanceInfo(
            OriginalArgs: [],
            Pid: 42,
            OwnerUser: "mysql",
            OwnerGroup: "mysql",
            ServiceName: "mysql.service");

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.SwapAsync(Context(layout, instanceInfo), CancellationToken.None));

        Assert.That(lifecycle.UnmaskedServices, Is.EqualTo(new[] { "mysql.service" }));
    }

    private static MysqlDatadirSwapCoordinator CreateCoordinator(
        RecordingSwapper swapper,
        RecordingLifecycle lifecycle,
        RecordingProbe? probe = null) =>
        new(
            NullLogger<MysqlDatadirSwapCoordinator>.Instance,
            swapper,
            lifecycle,
            probe ?? new RecordingProbe());

    private static MysqlDatadirSwapContext Context(
        SwapLayout layout,
        MysqlInstanceInfo? instanceInfo = null) =>
        new(
            Connection(),
            layout.RealDatadir,
            layout.StagingPath,
            layout.OldPath,
            layout.FailedPath,
            instanceInfo ?? InstanceInfo());

    private static MysqlInstanceInfo InstanceInfo() =>
        new(OriginalArgs: [], Pid: 42, OwnerUser: "mysql", OwnerGroup: "mysql", ServiceName: null);

    private static ConnectionConfig Connection() =>
        new()
        {
            Name = "mysql-main",
            Host = "127.0.0.1",
            Port = 3306,
            Username = "backup_user",
            Password = "password",
        };

    private static void AssertMarker(string dir, string expected)
    {
        Assert.That(Directory.Exists(dir), Is.True, $"Directory '{dir}' should exist.");
        Assert.That(File.ReadAllText(Path.Combine(dir, "marker.txt")), Is.EqualTo(expected));
    }

    private sealed class SwapLayout : IDisposable
    {
        private SwapLayout(string root)
        {
            Root = root;
            RealDatadir = Path.Combine(root, "mysql");
            StagingPath = Path.Combine(root, "mysql.new");
            OldPath = Path.Combine(root, "mysql.old");
            FailedPath = Path.Combine(root, "mysql.failed");
        }

        public string Root { get; }

        public string RealDatadir { get; }

        public string StagingPath { get; }

        public string OldPath { get; }

        public string FailedPath { get; }

        public static SwapLayout Create()
        {
            var layout = new SwapLayout(Path.Combine(
                Path.GetTempPath(),
                "backupster-mysql-swap-" + Guid.NewGuid().ToString("N")));

            Directory.CreateDirectory(layout.RealDatadir);
            Directory.CreateDirectory(layout.StagingPath);
            File.WriteAllText(Path.Combine(layout.RealDatadir, "marker.txt"), "old");
            File.WriteAllText(Path.Combine(layout.StagingPath, "marker.txt"), "new");
            return layout;
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Root))
                    Directory.Delete(Root, recursive: true);
            }
            catch
            {
            }
        }
    }

    private sealed class RecordingSwapper : IMysqlDatadirSwapper
    {
        public HashSet<string> MoveFailures { get; } = new(StringComparer.OrdinalIgnoreCase);

        public HashSet<string> TryMoveFailures { get; } = new(StringComparer.OrdinalIgnoreCase);

        public Exception? FixOwnershipException { get; init; }

        public void MoveDirectory(string from, string to)
        {
            if (MoveFailures.Contains(from))
                throw new InvalidOperationException($"Move failed from '{from}' to '{to}'.");

            Directory.Move(from, to);
        }

        public bool DirectoryExists(string path) => Directory.Exists(path);

        public Task FixOwnershipAsync(string newDatadir, MysqlInstanceInfo instanceInfo, CancellationToken ct)
        {
            if (FixOwnershipException is not null)
                throw FixOwnershipException;

            return Task.CompletedTask;
        }

        public bool TryMoveDirectory(string from, string to)
        {
            if (TryMoveFailures.Contains(from) || !Directory.Exists(from))
                return false;

            try
            {
                Directory.Move(from, to);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void TryDeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
            }
            catch
            {
            }
        }
    }

    private sealed class RecordingLifecycle : IMysqlLifecycleManager
    {
        public Queue<Exception> StartFailures { get; } = new();

        public List<string> StartDatadirs { get; } = [];

        public List<string> UnmaskedServices { get; } = [];

        public int StopCalls { get; private set; }

        public Action? OnStart { get; init; }

        public Task StopMysqlAsync(
            ConnectionConfig connection,
            MysqlInstanceInfo instanceInfo,
            CancellationToken ct,
            bool unmaskServiceOnFailure = true)
        {
            StopCalls++;
            return Task.CompletedTask;
        }

        public Task StartMysqlAsync(
            ConnectionConfig connection,
            string datadir,
            MysqlInstanceInfo instanceInfo,
            CancellationToken ct)
        {
            StartDatadirs.Add(datadir);
            OnStart?.Invoke();

            if (StartFailures.TryDequeue(out var ex))
                throw ex;

            return Task.CompletedTask;
        }

        public Task TryUnmaskServiceAsync(string serviceName)
        {
            UnmaskedServices.Add(serviceName);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingProbe : IMysqlServerProbe
    {
        public Task<int?> GetMysqlPidAsync(ConnectionConfig connection, CancellationToken ct) =>
            Task.FromResult<int?>(42);
    }
}
