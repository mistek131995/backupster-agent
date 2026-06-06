using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

public sealed class MysqlPhysicalProviderOsGuardTests
{
    [Test]
    public void BackupValidatePermissions_OnWindows_FailsBeforeResolvingXtraBackup()
    {
        SkipUnlessWindows();
        var runner = new ThrowingProcessRunner();
        var provider = CreateBackupProvider(runner);

        var ex = Assert.ThrowsAsync<BackupPermissionException>(() =>
            provider.ValidatePermissionsAsync(Connection(), "db", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("только на Linux"));
        Assert.That(runner.Calls, Is.Zero);
    }

    [Test]
    public void BackupAsync_OnWindows_FailsBeforeResolvingXtraBackup()
    {
        SkipUnlessWindows();
        var runner = new ThrowingProcessRunner();
        var provider = CreateBackupProvider(runner);

        var ex = Assert.ThrowsAsync<BackupPermissionException>(() =>
            provider.BackupAsync(Database(), Connection(), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("только на Linux"));
        Assert.That(runner.Calls, Is.Zero);
    }

    [Test]
    public void RestoreValidatePermissions_OnWindows_FailsBeforeResolvingXtraBackup()
    {
        SkipUnlessWindows();
        var runner = new ThrowingProcessRunner();
        var provider = CreateRestoreProvider(runner);

        var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
            provider.ValidatePermissionsAsync(Connection(), "db", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("только на Linux"));
        Assert.That(runner.Calls, Is.Zero);
    }

    [Test]
    public void RestoreAsync_OnWindows_FailsBeforeResolvingXtraBackup()
    {
        SkipUnlessWindows();
        var runner = new ThrowingProcessRunner();
        var provider = CreateRestoreProvider(runner);

        var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
            provider.RestoreAsync(Connection(), "db", "db", "backup.xbstream.gz", CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("только на Linux"));
        Assert.That(runner.Calls, Is.Zero);
    }

    private static MysqlPhysicalBackupProvider CreateBackupProvider(ThrowingProcessRunner runner) =>
        new(
            NullLogger<MysqlPhysicalBackupProvider>.Instance,
            new MysqlBinaryResolver(NullLogger<MysqlBinaryResolver>.Instance),
            runner);

    private static MysqlPhysicalRestoreProvider CreateRestoreProvider(ThrowingProcessRunner runner)
    {
        var probe = new MysqlServerProbe(NullLogger<MysqlServerProbe>.Instance);
        var binaryResolver = new MysqlBinaryResolver(NullLogger<MysqlBinaryResolver>.Instance);
        var lifecycle = new MysqlLifecycleManager(
            NullLogger<MysqlLifecycleManager>.Instance,
            probe,
            new MysqlSystemdController(new SystemdServiceController(
                NullLogger<SystemdServiceController>.Instance,
                runner,
                Options.Create(new RestoreSettings()))),
            binaryResolver);
        var swapper = new MysqlDatadirSwapper(
            NullLogger<MysqlDatadirSwapper>.Instance,
            Options.Create(new RestoreSettings()));

        return new(
            NullLogger<MysqlPhysicalRestoreProvider>.Instance,
            binaryResolver,
            probe,
            new MysqlBackupExtractor(NullLogger<MysqlBackupExtractor>.Instance, runner),
            new MysqlInstanceInspector(
                NullLogger<MysqlInstanceInspector>.Instance,
                probe,
                new SystemdUnitDetector(NullLogger<SystemdUnitDetector>.Instance),
                new MysqlSystemdController(new SystemdServiceController(
                    NullLogger<SystemdServiceController>.Instance,
                    runner,
                    Options.Create(new RestoreSettings())))),
            lifecycle,
            swapper,
            new MysqlDatadirSwapCoordinator(
                NullLogger<MysqlDatadirSwapCoordinator>.Instance,
                swapper,
                lifecycle,
                probe));
    }

    private static ConnectionConfig Connection() =>
        new()
        {
            Name = "mysql-main",
            Host = "127.0.0.1",
            Port = 3306,
            Username = "backup_user",
            Password = "password",
        };

    private static DatabaseConfig Database() =>
        new()
        {
            ConnectionName = "mysql-main",
            Database = "db",
            OutputPath = Path.Combine(Path.GetTempPath(), "backupster-mysql-physical-test"),
        };

    private static void SkipUnlessWindows()
    {
        if (!OperatingSystem.IsWindows())
            Assert.Ignore("Windows-only OS guard test.");
    }

    private sealed class ThrowingProcessRunner : IExternalProcessRunner
    {
        public int Calls { get; private set; }

        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct)
        {
            Calls++;
            throw new InvalidOperationException("External process runner should not be called.");
        }
    }
}
