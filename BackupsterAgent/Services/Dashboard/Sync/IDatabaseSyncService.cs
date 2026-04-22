namespace BackupsterAgent.Services.Dashboard.Sync;

public interface IDatabaseSyncService
{
    Task<bool> SyncAsync(CancellationToken ct = default);
}
