namespace BackupsterAgent.Services.Dashboard.Sync;

public interface IFileSetSyncService
{
    Task<bool> SyncAsync(CancellationToken ct = default);
}
