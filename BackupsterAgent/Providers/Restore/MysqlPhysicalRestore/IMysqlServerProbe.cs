using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public interface IMysqlServerProbe
{
    Task<int?> GetMysqlPidAsync(ConnectionConfig connection, CancellationToken ct);
}
