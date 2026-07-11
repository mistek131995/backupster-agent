using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;

namespace BackupsterAgent.Services.Dashboard.Clients;

public interface IBackupRecordClient
{
    Task<OpenRecordResult> OpenAsync(
        OpenBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);

    Task ReportProgressAsync(
        Guid backupRecordId,
        BackupProgressDto progress,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);

    Task<FinalizeRecordResult> FinalizeAsync(
        Guid backupRecordId,
        FinalizeBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);

    Task<LastSuccessfulLookupResult> GetLastSuccessfulAsync(
        string database,
        string storage,
        BackupMode mode,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);
}
