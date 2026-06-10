using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.State;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers.Handlers;

public sealed class BackupTaskHandler : IAgentTaskHandler
{
    private const string GenericBackupTaskErrorMessage =
        "Бэкап не выполнен. Подробности смотрите в логах агента.";

    private readonly IBackupJobRunner _backupJob;
    private readonly IBackupRunTracker _runTracker;
    private readonly StorageResolver _storages;
    private readonly List<DatabaseConfig> _databases;
    private readonly ILogger<BackupTaskHandler> _logger;

    public BackupTaskHandler(
        IBackupJobRunner backupJob,
        IBackupRunTracker runTracker,
        StorageResolver storages,
        IOptions<List<DatabaseConfig>> databases,
        ILogger<BackupTaskHandler> logger)
    {
        _backupJob = backupJob;
        _runTracker = runTracker;
        _storages = storages;
        _databases = databases.Value;
        _logger = logger;
    }

    public bool CanHandle(AgentTaskForAgentDto task) =>
        task.Type == AgentTaskType.Backup
        && string.IsNullOrWhiteSpace(task.Backup?.FileSetName);

    public async Task<PatchAgentTaskDto> HandleAsync(AgentTaskForAgentDto task, CancellationToken ct)
    {
        if (task.Backup is null)
        {
            _logger.LogWarning(
                "BackupTaskHandler: backup task {TaskId} has empty payload.", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = "Сервер не передал тело backup-задачи.",
            };
        }

        if (string.IsNullOrWhiteSpace(task.Backup.DatabaseName))
        {
            _logger.LogWarning(
                "BackupTaskHandler: backup task {TaskId} has no DatabaseName.", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = "Сервер не передал имя БД для backup-задачи.",
            };
        }

        var databaseName = task.Backup.DatabaseName;

        var config = _databases.FirstOrDefault(
            d => string.Equals(d.Database, databaseName, StringComparison.Ordinal));

        if (config is null)
        {
            _logger.LogWarning(
                "BackupTaskHandler: backup task {TaskId} references unknown database '{Database}'",
                task.Id, databaseName);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = $"БД '{databaseName}' не найдена в конфиге агента.",
            };
        }

        var mode = task.Backup.BackupMode;

        var effectiveStorageName = !string.IsNullOrWhiteSpace(task.Backup.StorageName)
            ? task.Backup.StorageName
            : config.StorageName;

        if (!_storages.TryResolve(effectiveStorageName, out var storage))
        {
            _logger.LogWarning(
                "BackupTaskHandler: backup task {TaskId} — storage '{Storage}' for database '{Database}' is not configured.",
                task.Id, effectiveStorageName, databaseName);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = $"Хранилище '{effectiveStorageName}' не настроено на агенте.",
            };
        }

        _logger.LogInformation(
            "BackupTaskHandler: executing backup task {TaskId} for database '{Database}' (mode={Mode}, storage={Storage}, baseRecordId={BaseRecordId})",
            task.Id, databaseName, mode, storage.Name, task.Backup.BaseBackupRecordId?.ToString() ?? "-");

        BackupResult result;
        try
        {
            result = await _backupJob.RunAsync(config, storage, mode, ct, task.Backup.BaseBackupRecordId);

            if (result.ChainBroken)
            {
                _logger.LogWarning(
                    "BackupTaskHandler: task {TaskId} DIFF for '{Database}' on '{Storage}' failed due to broken chain (failed DIFF record {DiffRecordId}). Running auto-FULL within the same lock scope (held by AgentTaskPollingService).",
                    task.Id, databaseName, storage.Name, result.BackupRecordId?.ToString() ?? "-");

                var autoFullResult = await _backupJob.RunAsync(
                    config, storage, BackupMode.Physical, ct, baseBackupRecordId: null);

                if (autoFullResult.Success)
                {
                    _logger.LogInformation(
                        "BackupTaskHandler: auto-rebase FULL succeeded for task {TaskId}. Database: '{Database}', Storage: '{Storage}', NewRecordId: {RecordId}, OriginalDiffRecordId: {DiffRecordId}",
                        task.Id, databaseName, storage.Name,
                        autoFullResult.BackupRecordId?.ToString() ?? "-",
                        result.BackupRecordId?.ToString() ?? "-");
                }
                else
                {
                    _logger.LogError(
                        "BackupTaskHandler: auto-rebase FULL failed for task {TaskId}. Database: '{Database}', Storage: '{Storage}', OriginalDiffRecordId: {DiffRecordId}, Error: {ErrorMessage}",
                        task.Id, databaseName, storage.Name,
                        result.BackupRecordId?.ToString() ?? "-", autoFullResult.ErrorMessage);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "BackupTaskHandler: backup task {TaskId} threw", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = GenericBackupTaskErrorMessage,
            };
        }

        _runTracker.RecordRun(
            IBackupRunTracker.DatabaseKey(databaseName, mode, storage.Name),
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
