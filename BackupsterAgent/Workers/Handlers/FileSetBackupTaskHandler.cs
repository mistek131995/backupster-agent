using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.State;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers.Handlers;

public sealed class FileSetBackupTaskHandler : IAgentTaskHandler
{
    private const string GenericFileSetBackupTaskErrorMessage =
        "Бэкап файлов не выполнен. Подробности смотрите в логах агента.";

    private readonly FileSetBackupJob _fileSetBackupJob;
    private readonly IBackupRunTracker _runTracker;
    private readonly StorageResolver _storages;
    private readonly List<FileSetConfig> _fileSets;
    private readonly ILogger<FileSetBackupTaskHandler> _logger;

    public FileSetBackupTaskHandler(
        FileSetBackupJob fileSetBackupJob,
        IBackupRunTracker runTracker,
        StorageResolver storages,
        IOptions<List<FileSetConfig>> fileSets,
        ILogger<FileSetBackupTaskHandler> logger)
    {
        _fileSetBackupJob = fileSetBackupJob;
        _runTracker = runTracker;
        _storages = storages;
        _fileSets = fileSets.Value;
        _logger = logger;
    }

    public bool CanHandle(AgentTaskForAgentDto task) =>
        task.Type == AgentTaskType.Backup
        && !string.IsNullOrWhiteSpace(task.Backup?.FileSetName);

    public async Task<PatchAgentTaskDto> HandleAsync(AgentTaskForAgentDto task, CancellationToken ct)
    {
        var fileSetName = task.Backup!.FileSetName!;

        var config = _fileSets.FirstOrDefault(
            f => string.Equals(f.Name, fileSetName, StringComparison.Ordinal));

        if (config is null)
        {
            _logger.LogWarning(
                "FileSetBackupTaskHandler: backup task {TaskId} references unknown file set '{Name}'",
                task.Id, fileSetName);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = $"Набор файлов '{fileSetName}' не найден в конфиге агента.",
            };
        }

        var effectiveStorageName = !string.IsNullOrWhiteSpace(task.Backup.StorageName)
            ? task.Backup.StorageName
            : config.StorageName;

        if (!_storages.TryResolve(effectiveStorageName, out var storage))
        {
            _logger.LogWarning(
                "FileSetBackupTaskHandler: backup task {TaskId} — storage '{Storage}' for file set '{Name}' is not configured.",
                task.Id, effectiveStorageName, fileSetName);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = $"Хранилище '{effectiveStorageName}' не настроено на агенте.",
            };
        }

        _logger.LogInformation(
            "FileSetBackupTaskHandler: executing file-set backup task {TaskId} for '{Name}' (storage={Storage})",
            task.Id, fileSetName, storage.Name);

        BackupResult result;
        try
        {
            result = await _fileSetBackupJob.RunAsync(config, storage, ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "FileSetBackupTaskHandler: file-set backup task {TaskId} threw", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = GenericFileSetBackupTaskErrorMessage,
            };
        }

        _runTracker.RecordRun(
            IBackupRunTracker.FileSetKey(fileSetName, storage.Name),
            DateTime.UtcNow);

        return result.Success
            ? new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Success,
                Backup = new BackupTaskResult { BackupRecordId = result.BackupRecordId },
            }
            : new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = result.ErrorMessage,
                Backup = new BackupTaskResult { BackupRecordId = result.BackupRecordId },
            };
    }
}
