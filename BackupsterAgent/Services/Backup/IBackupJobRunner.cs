using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;

namespace BackupsterAgent.Services.Backup;

public interface IBackupJobRunner
{
    Task<BackupResult> RunAsync(
        DatabaseConfig config,
        StorageConfig storage,
        BackupMode mode,
        CancellationToken ct,
        Guid? baseBackupRecordId = null);
}
