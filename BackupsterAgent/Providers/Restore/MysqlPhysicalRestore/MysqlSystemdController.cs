using BackupsterAgent.Providers.Restore.Common;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlSystemdController
{
    private const string Subject = "MySQL-сервис";

    private readonly SystemdServiceController _systemd;

    public MysqlSystemdController(SystemdServiceController systemd)
    {
        _systemd = systemd;
    }

    public Task MaskAsync(string serviceName) =>
        _systemd.MaskAsync(serviceName, Subject);

    public Task TryUnmaskAsync(string serviceName) =>
        _systemd.TryUnmaskAsync(serviceName, Subject);

    public Task StopAsync(string serviceName, CancellationToken ct) =>
        _systemd.StopAsync(serviceName, Subject, ct);

    public Task StartAsync(string serviceName, CancellationToken ct) =>
        _systemd.StartAsync(serviceName, Subject, ct);

    public Task<bool> IsActiveAsync(string serviceName, CancellationToken ct) =>
        _systemd.IsActiveAsync(serviceName, ct);

    public Task EnsureMainPidAsync(string serviceName, int expectedPid, CancellationToken ct) =>
        _systemd.EnsureMainPidAsync(serviceName, expectedPid, Subject, "mysqld", ct);
}
