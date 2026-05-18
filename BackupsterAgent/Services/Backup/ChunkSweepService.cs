using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common.Security;

namespace BackupsterAgent.Services.Backup;

public sealed class ChunkSweepService
{
    public const string ChunksPrefix = "chunks/";

    private readonly ManifestStore _manifestStore;
    private readonly EncryptionService _encryption;
    private readonly ILogger<ChunkSweepService> _logger;

    public ChunkSweepService(
        ManifestStore manifestStore,
        EncryptionService encryption,
        ILogger<ChunkSweepService> logger)
    {
        _manifestStore = manifestStore;
        _encryption = encryption;
        _logger = logger;
    }

    public async Task<ChunkSweepResult> SweepStorageAsync(
        IUploadProvider uploader,
        string storageName,
        TimeSpan graceWindow,
        CancellationToken ct)
    {
        if (!_encryption.IsConfigured)
        {
            _logger.LogWarning("ChunkGc: encryption key is not configured, sweep skipped.");
            return new ChunkSweepResult();
        }

        _logger.LogInformation(
            "ChunkGc: starting sweep for storage '{Storage}' (grace: {GraceH}h).",
            storageName, graceWindow.TotalHours);

        var referenced = new HashSet<string>(StringComparer.Ordinal);
        int manifestCount = 0;

        await foreach (var obj in uploader.ListAsync(string.Empty, ct))
        {
            if (!IsManifestKey(obj.Key)) continue;

            try
            {
                await using var reader = await _manifestStore.OpenReaderAsync(obj.Key, uploader, ct);

                await foreach (var entry in reader.ReadFilesAsync(ct))
                {
                    foreach (var chunk in entry.Chunks)
                        referenced.Add(chunk);
                }

                manifestCount++;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "ChunkGc: failed to read manifest '{Key}' in storage '{Storage}'. Aborting sweep.",
                    obj.Key, storageName);
                return new ChunkSweepResult { ManifestCount = manifestCount, ReferencedChunks = referenced.Count };
            }
        }

        var cutoff = DateTime.UtcNow - graceWindow;
        int totalChunks = 0;
        int deleted = 0;
        int skippedGrace = 0;
        long freedBytes = 0;

        await foreach (var obj in uploader.ListAsync(ChunksPrefix, ct))
        {
            totalChunks++;

            if (!obj.Key.StartsWith(ChunksPrefix, StringComparison.Ordinal)) continue;
            var sha = obj.Key[ChunksPrefix.Length..];
            if (sha.Length == 0) continue;
            if (referenced.Contains(sha)) continue;

            if (obj.LastModifiedUtc > cutoff)
            {
                skippedGrace++;
                continue;
            }

            try
            {
                await uploader.DeleteAsync(obj.Key, ct);
                deleted++;
                freedBytes += obj.Size;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "ChunkGc: failed to delete chunk '{Key}' in storage '{Storage}'.",
                    obj.Key, storageName);
            }
        }

        _logger.LogInformation(
            "ChunkGc: storage '{Storage}' — manifests: {Manifests}, referenced: {Refs}, total chunks: {Total}, deleted: {Deleted} ({FreedMb:F1} MB), skipped (grace): {Grace}.",
            storageName, manifestCount, referenced.Count, totalChunks, deleted, freedBytes / 1024.0 / 1024.0, skippedGrace);

        return new ChunkSweepResult
        {
            ManifestCount = manifestCount,
            ReferencedChunks = referenced.Count,
            TotalChunks = totalChunks,
            Deleted = deleted,
            SkippedGrace = skippedGrace,
            FreedBytes = freedBytes,
        };
    }

    private static bool IsManifestKey(string key) =>
        key.EndsWith(ManifestStore.NewSuffix, StringComparison.Ordinal) ||
        key.EndsWith(ManifestStore.LegacySuffix, StringComparison.Ordinal);
}
