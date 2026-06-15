using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlLifecycleManagerTests
{
    [Test]
    public void ResolveMysqld_UsesCapturedExecutablePathWhenAvailable()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "backupster-mysql-lifecycle-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        var mysqld = Path.Combine(tempDir, "mysqld");
        File.WriteAllText(mysqld, "");

        try
        {
            var lifecycle = CreateLifecycle(new RecordingSystemctlRunner());
            var instanceInfo = new MysqlInstanceInfo(
                OriginalArgs: [],
                Pid: 42,
                OwnerUser: "mysql",
                OwnerGroup: "mysql",
                ServiceName: null,
                MysqldPath: mysqld);

            var result = lifecycle.ResolveMysqld(Connection(), instanceInfo);

            Assert.That(result, Is.EqualTo(mysqld));
        }
        finally
        {
            try
            {
                if (Directory.Exists(tempDir))
                    Directory.Delete(tempDir, recursive: true);
            }
            catch
            {
            }
        }
    }

    [Test]
    public void TryTcpConnectAsync_CanceledToken_PropagatesCancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(() =>
            MysqlLifecycleManager.TryTcpConnectAsync("127.0.0.1", 3306, cts.Token));
    }

    [Test]
    public void StopMysql_ServiceStopFails_UnmasksByDefault()
    {
        var runner = new RecordingSystemctlRunner { ExitCodes = { ["stop"] = 1 } };
        var lifecycle = CreateLifecycle(runner);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            lifecycle.StopMysqlAsync(Connection(), ServiceInstance(), CancellationToken.None));

        Assert.That(runner.Verbs, Is.EqualTo(new[] { "mask", "stop", "unmask" }));
    }

    [Test]
    public void StopMysql_ServiceStopFails_DoesNotUnmaskWhenSuppressed()
    {
        var runner = new RecordingSystemctlRunner { ExitCodes = { ["stop"] = 1 } };
        var lifecycle = CreateLifecycle(runner);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            lifecycle.StopMysqlAsync(Connection(), ServiceInstance(), CancellationToken.None,
                unmaskServiceOnFailure: false));

        Assert.That(runner.Verbs, Is.EqualTo(new[] { "mask", "stop" }));
        Assert.That(runner.Verbs, Does.Not.Contain("unmask"));
    }

    [Test]
    public void StopMysql_MaskFails_UnmasksByDefault()
    {
        var runner = new RecordingSystemctlRunner { ExitCodes = { ["mask"] = 1 } };
        var lifecycle = CreateLifecycle(runner);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            lifecycle.StopMysqlAsync(Connection(), ServiceInstance(), CancellationToken.None));

        Assert.That(runner.Verbs, Is.EqualTo(new[] { "mask", "unmask" }));
    }

    [Test]
    public void StopMysql_WhenMaskHangs_TimesOutAndUnmasks()
    {
        var runner = new RecordingSystemctlRunner { HangingVerbs = { "mask" } };
        var lifecycle = CreateLifecycle(runner, timeoutSeconds: 1);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            lifecycle.StopMysqlAsync(Connection(), ServiceInstance(), CancellationToken.None));

        Assert.That(runner.Verbs, Is.EqualTo(new[] { "mask", "unmask" }));
    }

    private static MysqlLifecycleManager CreateLifecycle(RecordingSystemctlRunner runner, int? timeoutSeconds = null)
    {
        var settings = timeoutSeconds.HasValue
            ? new RestoreSettings { SystemctlTimeoutSeconds = timeoutSeconds.Value }
            : new RestoreSettings();

        return new(
            NullLogger<MysqlLifecycleManager>.Instance,
            new MysqlServerProbe(NullLogger<MysqlServerProbe>.Instance),
            new MysqlSystemdController(new SystemdServiceController(
                NullLogger<SystemdServiceController>.Instance,
                runner,
                Options.Create(settings))),
            new MysqlBinaryResolver(NullLogger<MysqlBinaryResolver>.Instance));
    }

    private static MysqlInstanceInfo ServiceInstance() =>
        new(OriginalArgs: [], Pid: null, OwnerUser: null, OwnerGroup: null, ServiceName: "mysql.service");

    private static ConnectionConfig Connection() =>
        new()
        {
            Name = "mysql-main",
            Host = "127.0.0.1",
            Port = 3306,
            Username = "backup_user",
            Password = "password",
        };
}
