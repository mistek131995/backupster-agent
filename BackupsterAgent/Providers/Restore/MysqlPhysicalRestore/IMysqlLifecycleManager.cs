using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public interface IMysqlLifecycleManager
{
    Task StopMysqlAsync(
        ConnectionConfig connection,
        MysqlInstanceInfo instanceInfo,
        CancellationToken ct,
        bool unmaskServiceOnFailure = true);

    Task StartMysqlAsync(
        ConnectionConfig connection,
        string datadir,
        MysqlInstanceInfo instanceInfo,
        CancellationToken ct);

    Task TryUnmaskServiceAsync(string serviceName);
}
