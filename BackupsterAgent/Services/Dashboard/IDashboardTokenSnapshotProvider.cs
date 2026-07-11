namespace BackupsterAgent.Services.Dashboard;

public interface IDashboardTokenSnapshotProvider
{
    Task<DashboardTokenSnapshot> CaptureAsync(CancellationToken ct);
}
