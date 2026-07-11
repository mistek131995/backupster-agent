using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Backup.Coordinator;
using BackupsterAgent.Services.Dashboard;

namespace BackupsterAgent.Services.Backup;

public sealed class FileSetBackupJob
{
    private readonly BackupRunCoordinator _coordinator;
    private readonly FileSetBackupPipeline _pipeline;

    public FileSetBackupJob(BackupRunCoordinator coordinator, FileSetBackupPipeline pipeline)
    {
        _coordinator = coordinator;
        _pipeline = pipeline;
    }

    public Task<BackupResult> RunAsync(
        FileSetConfig config,
        StorageConfig storage,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null) =>
        _coordinator.RunAsync(
            new FileSetBackupDescriptor(config, storage, _pipeline), ct, tokenSnapshot);
}
