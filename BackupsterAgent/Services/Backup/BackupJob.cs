using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Backup.Coordinator;

namespace BackupsterAgent.Services.Backup;

public class BackupJob : IBackupJobRunner
{
    private readonly BackupRunCoordinator _coordinator;
    private readonly DatabaseBackupPipeline _pipeline;

    public BackupJob(BackupRunCoordinator coordinator, DatabaseBackupPipeline pipeline)
    {
        _coordinator = coordinator;
        _pipeline = pipeline;
    }

    public virtual Task<BackupResult> RunAsync(
        DatabaseConfig config,
        StorageConfig storage,
        BackupMode mode,
        CancellationToken ct,
        Guid? baseBackupRecordId = null) =>
        _coordinator.RunAsync(
            new DatabaseBackupDescriptor(config, storage, mode, _pipeline, baseBackupRecordId), ct);
}
