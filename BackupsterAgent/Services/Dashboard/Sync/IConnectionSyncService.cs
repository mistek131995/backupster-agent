namespace BackupsterAgent.Services.Dashboard.Sync;

public interface IConnectionSyncService
{
    Task<bool> SyncAsync(CancellationToken ct = default);
}
