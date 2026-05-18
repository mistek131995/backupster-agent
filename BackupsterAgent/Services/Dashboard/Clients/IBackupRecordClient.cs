using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;

namespace BackupsterAgent.Services.Dashboard.Clients;

public interface IBackupRecordClient
{
    Task<OpenRecordResult> OpenAsync(OpenBackupRecordDto dto, CancellationToken ct);

    Task ReportProgressAsync(Guid backupRecordId, BackupProgressDto progress, CancellationToken ct);

    Task<FinalizeRecordResult> FinalizeAsync(Guid backupRecordId, FinalizeBackupRecordDto dto, CancellationToken ct);

    Task<LastSuccessfulLookupResult> GetLastSuccessfulAsync(
        string database,
        string storage,
        BackupMode mode,
        CancellationToken ct);
}
