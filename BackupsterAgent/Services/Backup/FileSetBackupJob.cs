using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Upload;

namespace BackupsterAgent.Services.Backup;

public sealed class FileSetBackupJob
{
    private readonly StorageResolver _storages;
    private readonly IUploadServiceFactory _uploadFactory;
    private readonly FileBackupService _fileBackup;
    private readonly ManifestStore _manifestStore;
    private readonly IBackupRecordClient _recordClient;
    private readonly IProgressReporterFactory _reporterFactory;
    private readonly IOutboxStore _outboxStore;
    private readonly ActivitySource _activitySource;
    private readonly ILogger<FileSetBackupJob> _logger;

    public FileSetBackupJob(
        StorageResolver storages,
        IUploadServiceFactory uploadFactory,
        FileBackupService fileBackup,
        ManifestStore manifestStore,
        IBackupRecordClient recordClient,
        IProgressReporterFactory reporterFactory,
        IOutboxStore outboxStore,
        ActivitySource activitySource,
        ILogger<FileSetBackupJob> logger)
    {
        _storages = storages;
        _uploadFactory = uploadFactory;
        _fileBackup = fileBackup;
        _manifestStore = manifestStore;
        _recordClient = recordClient;
        _reporterFactory = reporterFactory;
        _outboxStore = outboxStore;
        _activitySource = activitySource;
        _logger = logger;
    }

    public async Task<BackupResult> RunAsync(FileSetConfig config, CancellationToken ct)
    {
        using var activity = _activitySource.StartActivity("fileset.backup.run");
        activity?.SetTag("fileSet", config.Name);
        activity?.SetTag("storage", config.StorageName);

        _logger.LogInformation(
            "FileSetBackupJob starting. FileSet: '{Name}', Storage: '{Storage}', Paths: {PathCount}, TraceId: {TraceId}",
            config.Name, config.StorageName, config.Paths.Count, activity?.TraceId.ToString() ?? "-");

        var startedAt = DateTime.UtcNow;

        var openResult = await OpenRecordAsync(config, startedAt, ct);

        if (openResult.Status == DashboardAvailability.PermanentSkip)
        {
            return new BackupResult
            {
                Success = false,
                ErrorMessage = "Could not open backup record on dashboard — run skipped.",
            };
        }

        var offline = openResult.Status == DashboardAvailability.OfflineRetryable;
        var recordId = openResult.Id;
        string? clientTaskId = offline ? Guid.NewGuid().ToString() : null;

        if (offline)
        {
            _logger.LogWarning(
                "FileSetBackupJob: dashboard offline at start for '{Name}' — switching to offline mode (clientTaskId={ClientTaskId})",
                config.Name, clientTaskId);
        }

        await using var reporter = _reporterFactory.CreateForBackup(recordId ?? Guid.Empty, offline: offline);

        BackupResult result;
        string? fileError = null;
        CaptureMetrics? metrics = null;
        bool cancelled = false;

        try
        {
            var storage = _storages.Resolve(config.StorageName);

            if (storage.Provider == UploadProvider.Sftp)
            {
                const string error = "File backup is not supported on SFTP storage. Configure an S3 storage for this file set.";
                _logger.LogWarning(
                    "FileSetBackupJob: SFTP storage '{Storage}' is not supported for file sets. FileSet '{Name}' skipped.",
                    storage.Name, config.Name);
                fileError = "Бэкап файлов не поддерживается на SFTP-хранилище. Настройте S3-хранилище для этого набора файлов.";
                result = new BackupResult
                {
                    Success = false,
                    ErrorMessage = error,
                    BackupRecordId = recordId,
                };
            }
            else
            {
                var uploader = _uploadFactory.GetService(config.StorageName);
                var backupFolder = $"{config.Name}/{startedAt:yyyy-MM-dd_HH-mm-ss}";

                _logger.LogInformation("FileSetBackupJob resolved. Folder: '{Folder}'", backupFolder);

                reporter.Report(BackupStage.CapturingFiles);

                await using var writer = _manifestStore.OpenWriter(config.Name, dumpObjectKey: string.Empty);
                var capture = await _fileBackup.CaptureAsync(config.Paths, uploader, writer, reporter, ct);
                var manifestKey = await writer.CompleteAsync(uploader, backupFolder, ct);

                metrics = new CaptureMetrics
                {
                    ManifestKey = manifestKey,
                    FilesCount = checked((int)writer.FilesCount),
                    FilesTotalBytes = writer.FilesTotalBytes,
                    NewChunksCount = capture.NewChunksCount,
                    DurationMs = (long)(DateTime.UtcNow - startedAt).TotalMilliseconds,
                };

                result = new BackupResult
                {
                    Success = true,
                    SizeBytes = metrics.FilesTotalBytes,
                    DurationMs = metrics.DurationMs,
                    BackupRecordId = recordId,
                };

                _logger.LogInformation(
                    "FileSetBackupJob captured. FileSet: '{Name}', Files: {FilesCount}, TotalBytes: {TotalBytes}, NewChunks: {NewChunks}",
                    config.Name, metrics.FilesCount, metrics.FilesTotalBytes, metrics.NewChunksCount);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("FileSetBackupJob cancelled mid-pipeline for '{Name}'", config.Name);
            result = new BackupResult
            {
                Success = false,
                ErrorMessage = "Бэкап прерван: агент остановлен.",
                BackupRecordId = recordId,
            };
            cancelled = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FileSetBackupJob failed for '{Name}'", config.Name);
            result = new BackupResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                BackupRecordId = recordId,
            };
        }

        var finalizeDto = BuildFinalizeDto(result, metrics, fileError);

        if (offline)
        {
            await EnqueueOutboxAsync(
                clientTaskId!, config, startedAt, finalizeDto, serverRecordId: null, ct);
        }
        else
        {
            var finalizeResult = await FinalizeRecordAsync(
                recordId!.Value, finalizeDto, ct, cancelled, config.Name);

            if (finalizeResult.Status == DashboardAvailability.OfflineRetryable && !cancelled)
            {
                clientTaskId = Guid.NewGuid().ToString();
                _logger.LogWarning(
                    "FileSetBackupJob: dashboard offline at finalize for '{Name}' — entry queued (clientTaskId={ClientTaskId}, serverRecordId={RecordId})",
                    config.Name, clientTaskId, recordId);
                await EnqueueOutboxAsync(
                    clientTaskId, config, startedAt, finalizeDto, serverRecordId: recordId, ct);
            }
        }

        if (cancelled) throw new OperationCanceledException(ct);

        return result;
    }

    private async Task<OpenRecordResult> OpenRecordAsync(
        FileSetConfig config, DateTime startedAt, CancellationToken ct)
    {
        var result = await _recordClient.OpenAsync(
            new OpenBackupRecordDto
            {
                DatabaseName = config.Name,
                ConnectionName = string.Empty,
                StorageName = config.StorageName,
                StartedAt = startedAt,
                DatabaseType = BackupsterAgent.Enums.DatabaseType.FileSet,
                FileSetName = config.Name,
            }, ct);

        if (result.Status == DashboardAvailability.PermanentSkip)
        {
            _logger.LogWarning(
                "FileSetBackupJob: dashboard rejected open for '{Name}' (permanent skip). Run skipped.",
                config.Name);
        }

        return result;
    }

    private async Task<FinalizeRecordResult> FinalizeRecordAsync(
        Guid recordId,
        FinalizeBackupRecordDto dto,
        CancellationToken runCt,
        bool cancelled,
        string fileSetName)
    {
        using var cancelFinalizeCts = cancelled ? new CancellationTokenSource(TimeSpan.FromSeconds(10)) : null;
        var finalizeCt = cancelled ? cancelFinalizeCts!.Token : runCt;

        try
        {
            return await _recordClient.FinalizeAsync(recordId, dto, finalizeCt);
        }
        catch (Exception ex) when (cancelled)
        {
            _logger.LogError(ex,
                "FileSetBackupJob: could not finalize cancelled record for '{Name}'. Sweeper will close it.",
                fileSetName);
            return new FinalizeRecordResult(DashboardAvailability.PermanentSkip);
        }
    }

    private async Task EnqueueOutboxAsync(
        string clientTaskId,
        FileSetConfig config,
        DateTime startedAt,
        FinalizeBackupRecordDto finalizeDto,
        Guid? serverRecordId,
        CancellationToken ct)
    {
        var entry = new OutboxEntry
        {
            ClientTaskId = clientTaskId,
            DatabaseName = config.Name,
            ConnectionName = string.Empty,
            StorageName = config.StorageName,
            StartedAt = startedAt,
            BackupAt = finalizeDto.BackupAt,
            Status = finalizeDto.Status == BackupStatus.Success ? "success" : "failed",
            SizeBytes = finalizeDto.SizeBytes,
            DurationMs = finalizeDto.DurationMs,
            DumpObjectKey = finalizeDto.DumpObjectKey,
            ErrorMessage = finalizeDto.ErrorMessage,
            ManifestKey = finalizeDto.ManifestKey,
            FilesCount = finalizeDto.FilesCount,
            FilesTotalBytes = finalizeDto.FilesTotalBytes,
            NewChunksCount = finalizeDto.NewChunksCount,
            FileBackupError = finalizeDto.FileBackupError,
            QueuedAt = DateTime.UtcNow,
            AttemptCount = 0,
            ServerRecordId = serverRecordId,
            DatabaseType = BackupsterAgent.Enums.DatabaseType.FileSet,
            FileSetName = config.Name,
        };

        try
        {
            await _outboxStore.EnqueueAsync(entry, ct);
            _logger.LogInformation(
                "FileSetBackupJob: outbox entry saved (clientTaskId={ClientTaskId}, fileSet={Name}, status={Status}, serverRecordId={ServerId})",
                clientTaskId, config.Name, entry.Status, serverRecordId?.ToString() ?? "-");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "FileSetBackupJob: failed to save outbox entry for '{Name}'. Backup completed but won't be replayed.",
                config.Name);
        }
    }

    private static FinalizeBackupRecordDto BuildFinalizeDto(
        BackupResult result, CaptureMetrics? metrics, string? fileBackupError) =>
        new()
        {
            Status = result.Success ? BackupStatus.Success : BackupStatus.Failed,
            SizeBytes = metrics?.FilesTotalBytes,
            DurationMs = metrics?.DurationMs,
            DumpObjectKey = null,
            ErrorMessage = result.ErrorMessage,
            BackupAt = DateTime.UtcNow,
            ManifestKey = metrics?.ManifestKey,
            FilesCount = metrics?.FilesCount,
            FilesTotalBytes = metrics?.FilesTotalBytes,
            NewChunksCount = metrics?.NewChunksCount,
            FileBackupError = fileBackupError,
        };

    private sealed class CaptureMetrics
    {
        public required string ManifestKey { get; init; }
        public required int FilesCount { get; init; }
        public required long FilesTotalBytes { get; init; }
        public required int NewChunksCount { get; init; }
        public required long DurationMs { get; init; }
    }
}
