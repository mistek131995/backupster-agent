using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

public interface IPostgresReadinessProbe
{
    Task WaitUntilReadyAsync(ConnectionConfig connection, TimeSpan timeout, CancellationToken ct);
}
