using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Progress;

namespace BackupsterAgent.Services.Backup;

public sealed class BackupDeleteService(IUploadProviderFactory uploadFactory, ILogger<BackupDeleteService> logger)
{
    public async Task<BackupDeleteResult> RunAsync(Guid correlationId, DeleteTaskPayload payload, 
        IProgressReporter<DeleteStage>? reporter, CancellationToken ct)
    {
        logger.LogInformation(
            "BackupDeleteService starting. Correlation: {CorrelationId}, Storage: '{Storage}', Dump: '{Dump}', Manifest: '{Manifest}'",
            correlationId, payload.StorageName, payload.DumpObjectKey ?? "(none)", payload.ManifestKey ?? "(none)");

        reporter?.Report(DeleteStage.Resolving);

        IUploadProvider uploader;
        try
        {
            uploader = uploadFactory.GetProvider(payload.StorageName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "BackupDeleteService: failed to resolve storage '{Storage}' for {CorrelationId}",
                payload.StorageName, correlationId);
            return BackupDeleteResult.Failed(
                $"Хранилище '{payload.StorageName}' не найдено в конфиге агента. " +
                "Добавьте его в Storages[] либо удалите запись через force-drop.");
        }

        if (!string.IsNullOrWhiteSpace(payload.ManifestKey))
        {
            reporter?.Report(DeleteStage.DeletingManifest, currentItem: payload.ManifestKey);
            try
            {
                await uploader.DeleteAsync(payload.ManifestKey, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "BackupDeleteService: failed to delete manifest '{Key}' for {CorrelationId}",
                    payload.ManifestKey, correlationId);
                return BackupDeleteResult.Failed(
                    $"Не удалось удалить манифест '{payload.ManifestKey}' из хранилища: {ex.Message}");
            }
        }

        if (!string.IsNullOrWhiteSpace(payload.DumpObjectKey))
        {
            reporter?.Report(DeleteStage.DeletingDump, currentItem: payload.DumpObjectKey);
            try
            {
                await uploader.DeleteAsync(payload.DumpObjectKey, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "BackupDeleteService: failed to delete dump '{Key}' for {CorrelationId}",
                    payload.DumpObjectKey, correlationId);
                return BackupDeleteResult.Failed(
                    $"Не удалось удалить дамп '{payload.DumpObjectKey}' из хранилища: {ex.Message}");
            }
        }

        reporter?.Report(DeleteStage.Completed);

        logger.LogInformation(
            "BackupDeleteService completed. Correlation: {CorrelationId}, Storage: '{Storage}'",
            correlationId, payload.StorageName);

        return BackupDeleteResult.Success();
    }
}
