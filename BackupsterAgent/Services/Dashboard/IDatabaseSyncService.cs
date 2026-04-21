namespace BackupsterAgent.Services.Dashboard;

public interface IDatabaseSyncService
{
    Task<bool> SyncAsync(CancellationToken ct = default);
}
