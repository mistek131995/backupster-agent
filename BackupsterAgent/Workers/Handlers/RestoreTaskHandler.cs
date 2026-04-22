using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Restore;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers.Handlers;

public sealed class RestoreTaskHandler : IAgentTaskHandler
{
    private readonly DatabaseRestoreService _databaseRestore;
    private readonly FileRestoreService _fileRestore;
    private readonly IUploadProviderFactory _uploadFactory;
    private readonly IProgressReporterFactory _reporterFactory;
    private readonly List<DatabaseConfig> _databases;
    private readonly ILogger<RestoreTaskHandler> _logger;

    public RestoreTaskHandler(
        DatabaseRestoreService databaseRestore,
        FileRestoreService fileRestore,
        IUploadProviderFactory uploadFactory,
        IProgressReporterFactory reporterFactory,
        IOptions<List<DatabaseConfig>> databases,
        ILogger<RestoreTaskHandler> logger)
    {
        _databaseRestore = databaseRestore;
        _fileRestore = fileRestore;
        _uploadFactory = uploadFactory;
        _reporterFactory = reporterFactory;
        _databases = databases.Value;
        _logger = logger;
    }

    public bool CanHandle(AgentTaskForAgentDto task) => task.Type == AgentTaskType.Restore;

    public async Task<PatchAgentTaskDto> HandleAsync(AgentTaskForAgentDto task, CancellationToken ct)
    {
        if (task.Restore is null)
        {
            _logger.LogWarning(
                "RestoreTaskHandler: restore task {TaskId} has empty payload.", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = "Сервер не передал тело restore-задачи.",
                Restore = new RestoreTaskResult { DatabaseStatus = RestoreDatabaseStatus.Failed },
            };
        }

        var payload = task.Restore;
        var isFileSet = string.IsNullOrWhiteSpace(payload.DumpObjectKey);

        _logger.LogInformation(
            "RestoreTaskHandler: executing restore task {TaskId} (source '{Source}', target '{Target}', fileSet={IsFileSet})",
            task.Id, payload.SourceDatabaseName,
            payload.TargetDatabaseName ?? payload.SourceDatabaseName, isFileSet);

        if (isFileSet && string.IsNullOrWhiteSpace(payload.ManifestKey))
        {
            _logger.LogWarning(
                "RestoreTaskHandler: restore task {TaskId} has neither DumpObjectKey nor ManifestKey",
                task.Id);
            return FailRestore("В задаче восстановления нет ни дампа, ни манифеста файлов.");
        }

        if (ValidateTaskNames(payload) is { } validationError)
        {
            _logger.LogWarning(
                "RestoreTaskHandler: restore task {TaskId} rejected by name validation: {Reason}",
                task.Id, validationError);
            return FailRestore(validationError);
        }

        await using var reporter = _reporterFactory.CreateForRestore(task.Id);

        IUploadProvider uploader;
        try
        {
            uploader = ResolveUploader(payload);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "RestoreTaskHandler: failed to resolve storage for task {TaskId}", task.Id);
            return FailRestore(ex.Message);
        }

        var dbResult = isFileSet
            ? DatabaseRestoreResult.Success()
            : await _databaseRestore.RunAsync(task.Id, payload, uploader, reporter, ct);

        var fileResult = payload.ManifestKey is null
            ? FileRestoreResult.Skipped()
            : await _fileRestore.RunAsync(payload.ManifestKey, payload.TargetFileRoot, uploader, reporter, ct);

        return CombineResults(dbResult, fileResult);
    }

    private static PatchAgentTaskDto FailRestore(string message) => new()
    {
        Status = AgentTaskStatus.Failed,
        ErrorMessage = message,
        Restore = new RestoreTaskResult { DatabaseStatus = RestoreDatabaseStatus.Failed },
    };

    internal static string? ValidateTaskNames(RestoreTaskPayload payload)
    {
        if (!DatabaseNameValidator.IsValid(payload.SourceDatabaseName, out var sourceReason))
            return $"Имя исходной БД не прошло валидацию: {sourceReason}.";

        if (!string.IsNullOrEmpty(payload.TargetDatabaseName)
            && !DatabaseNameValidator.IsValid(payload.TargetDatabaseName, out var targetReason))
        {
            return $"Имя целевой БД не прошло валидацию: {targetReason}.";
        }

        return null;
    }

    internal IUploadProvider ResolveUploader(RestoreTaskPayload payload)
    {
        var storageName = payload.StorageName;

        if (string.IsNullOrWhiteSpace(storageName))
        {
            var dbConfig = _databases.FirstOrDefault(
                d => string.Equals(d.Database, payload.SourceDatabaseName, StringComparison.Ordinal));

            if (dbConfig is null)
            {
                throw new InvalidOperationException(
                    $"БД '{payload.SourceDatabaseName}' не найдена в конфиге агента, а дашборд не передал StorageName. " +
                    "Добавьте БД в конфиг либо обновите дашборд, чтобы он передавал имя хранилища.");
            }

            storageName = dbConfig.StorageName;
        }

        return _uploadFactory.GetProvider(storageName);
    }

    internal static PatchAgentTaskDto CombineResults(DatabaseRestoreResult db, FileRestoreResult files)
    {
        var databaseStatus = db.IsSuccess ? RestoreDatabaseStatus.Success : RestoreDatabaseStatus.Failed;
        var filesStatus = files.Status;

        AgentTaskStatus overallStatus;
        if (!db.IsSuccess)
            overallStatus = AgentTaskStatus.Failed;
        else if (filesStatus is RestoreFilesStatus.Failed or RestoreFilesStatus.Partial)
            overallStatus = AgentTaskStatus.Partial;
        else
            overallStatus = AgentTaskStatus.Success;

        string? errorMessage;
        if (db.ErrorMessage is not null && files.ErrorMessage is not null)
            errorMessage = $"{db.ErrorMessage}\n\n{files.ErrorMessage}";
        else
            errorMessage = db.ErrorMessage ?? files.ErrorMessage;

        return new PatchAgentTaskDto
        {
            Status = overallStatus,
            ErrorMessage = errorMessage,
            Restore = new RestoreTaskResult
            {
                DatabaseStatus = databaseStatus,
                FilesStatus = filesStatus,
                FilesRestoredCount = files.FilesRestoredCount > 0 ? files.FilesRestoredCount : null,
                FilesFailedCount = files.FilesFailedCount > 0 ? files.FilesFailedCount : null,
            },
        };
    }
}
