namespace BackupsterAgent.Services.Dashboard;

public interface IFileSetSyncService
{
    Task<bool> SyncAsync(CancellationToken ct = default);
}
