using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Dashboard;

namespace BackupsterAgent.Services.Backup;

public interface IBackupJobRunner
{
    Task<BackupResult> RunAsync(
        DatabaseConfig config,
        StorageConfig storage,
        BackupMode mode,
        CancellationToken ct,
        Guid? baseBackupRecordId = null,
        DashboardTokenSnapshot? tokenSnapshot = null);
}
