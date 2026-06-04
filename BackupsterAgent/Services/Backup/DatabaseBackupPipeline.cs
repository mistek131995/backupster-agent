using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup.Coordinator;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Security;

namespace BackupsterAgent.Services.Backup;

public sealed class DatabaseBackupPipeline
{
    private readonly IBackupProviderFactory _factory;
    private readonly ConnectionResolver _connections;
    private readonly EncryptionService _encryption;
    private readonly IUploadProviderFactory _uploadFactory;
    private readonly FileBackupService _fileBackup;
    private readonly ManifestStore _manifestStore;
    private readonly ILogger<DatabaseBackupPipeline> _logger;

    public DatabaseBackupPipeline(
        IBackupProviderFactory factory,
        ConnectionResolver connections,
        EncryptionService encryption,
        IUploadProviderFactory uploadFactory,
        FileBackupService fileBackup,
        ManifestStore manifestStore,
        ILogger<DatabaseBackupPipeline> logger)
    {
        _factory = factory;
        _connections = connections;
        _encryption = encryption;
        _uploadFactory = uploadFactory;
        _fileBackup = fileBackup;
        _manifestStore = manifestStore;
        _logger = logger;
    }

    public async Task<PipelineOutcome> ExecuteAsync(
        BackupRunExecution exec,
        DatabaseConfig config,
        StorageConfig storage,
        BackupMode mode,
        Guid? baseBackupRecordId,
        CancellationToken ct)
    {
        var startedAt = exec.StartedAt;
        string? dumpFile = null;
        string? encryptedFile = null;
        string? pgManifestFile = null;
        string? encryptedPgManifest = null;
        string? decryptedBaseManifest = null;
        string? decryptedBaseManifestWorkDir = null;
        string? dumpObjectKey = null;
        string? pgBaseManifestKey = null;
        string? backupFolder = null;
        IUploadProvider uploader;
        long sizeBytes;
        long durationMs;

        try
        {
            var connection = _connections.Resolve(config.ConnectionName);
            uploader = _uploadFactory.GetProvider(storage.Name);
            backupFolder = $"{config.DatabasePathSegment}/{startedAt:yyyy-MM-dd_HH-mm-ss}";

            BackupResult dumpResult;
            if (mode == BackupMode.PhysicalDifferential)
            {
                if (baseBackupRecordId is null || baseBackupRecordId.Value == Guid.Empty)
                    throw new InvalidOperationException(
                        "Дифференциальный бэкап невозможен: дашборд не передал идентификатор родительского полного бэкапа.");

                var diffProvider = _factory.GetDifferentialProvider(connection.DatabaseType);

                _logger.LogInformation(
                    "DatabaseBackupPipeline resolved (differential). Provider: {ProviderType}, Folder: '{Folder}', BaseRecordId: {BaseRecordId}",
                    diffProvider.GetType().Name, backupFolder, baseBackupRecordId);

                await diffProvider.ValidatePermissionsAsync(connection, config.Database, ct);

                if (connection.DatabaseType == DatabaseType.Postgres)
                {
                    if (string.IsNullOrWhiteSpace(exec.BasePgBaseManifestKey))
                        throw new InvalidOperationException(
                            "Дифференциальный бэкап PostgreSQL невозможен: дашборд не передал ключ backup_manifest родительского бэкапа.");

                    var baseManifest = await DownloadAndDecryptManifestAsync(
                        uploader, exec.BasePgBaseManifestKey, config, ct);
                    decryptedBaseManifest = baseManifest.DecryptedPath;
                    decryptedBaseManifestWorkDir = baseManifest.WorkDir;
                }

                var context = new DifferentialBackupContext
                {
                    BaseBackupRecordId = baseBackupRecordId.Value,
                    BaseDumpObjectKey = exec.BaseDumpObjectKey,
                    BasePgBaseManifestPath = decryptedBaseManifest,
                };

                _logger.LogInformation("Step 1/3: dump (differential)");
                exec.Reporter.Report(BackupStage.Dumping);
                dumpResult = await diffProvider.BackupAsync(config, connection, context, ct);
            }
            else
            {
                var provider = _factory.GetProvider(connection.DatabaseType, mode);

                _logger.LogInformation(
                    "DatabaseBackupPipeline resolved. Provider: {ProviderType}, Folder: '{Folder}'",
                    provider.GetType().Name, backupFolder);

                await provider.ValidatePermissionsAsync(connection, config.Database, ct);

                _logger.LogInformation("Step 1/3: dump");
                exec.Reporter.Report(BackupStage.Dumping);
                dumpResult = await provider.BackupAsync(config, connection, ct);
            }

            dumpFile = dumpResult.FilePath;
            pgManifestFile = dumpResult.PgBaseManifestPath;
            sizeBytes = dumpResult.SizeBytes;
            durationMs = dumpResult.DurationMs;

            _logger.LogInformation("Step 2/3: encrypt");
            exec.Reporter.Report(BackupStage.EncryptingDump);
            encryptedFile = await _encryption.EncryptAsync(dumpFile, ct);

            if (pgManifestFile is not null)
                encryptedPgManifest = await _encryption.EncryptAsync(pgManifestFile, ct);

            _logger.LogInformation("Step 3/3: upload");
            exec.Reporter.Report(BackupStage.UploadingDump, processed: 0, unit: "bytes");
            var uploadProgress = new Progress<long>(bytes =>
                exec.Reporter.Report(BackupStage.UploadingDump, processed: bytes, unit: "bytes"));
            await uploader.UploadAsync(encryptedFile, backupFolder, uploadProgress, ct);
            dumpObjectKey = $"{backupFolder}/{Path.GetFileName(encryptedFile)}";

            if (encryptedPgManifest is not null)
            {
                await uploader.UploadAsync(encryptedPgManifest, backupFolder, progress: null, ct);
                pgBaseManifestKey = $"{backupFolder}/{Path.GetFileName(encryptedPgManifest)}";
                _logger.LogInformation(
                    "PostgreSQL backup_manifest uploaded. Key: '{Key}'", pgBaseManifestKey);
            }

            _logger.LogInformation(
                "Dump uploaded. File: '{FilePath}', Size: {SizeBytes} bytes, " +
                "Duration: {DurationMs} ms, DumpObjectKey: '{DumpObjectKey}'",
                dumpFile, sizeBytes, durationMs, dumpObjectKey);
        }
        finally
        {
            TryDelete(dumpFile);
            TryDelete(encryptedFile);
            TryDelete(pgManifestFile);
            TryDelete(encryptedPgManifest);
            TryDelete(decryptedBaseManifest);
            TryDeleteDirectory(decryptedBaseManifestWorkDir);
        }

        var (fileMetrics, fileError) = await CaptureFilesSafelyAsync(
            config, storage, uploader, backupFolder, dumpObjectKey, exec.Reporter, ct);

        return new PipelineOutcome
        {
            Success = true,
            FilePath = dumpFile,
            SizeBytes = sizeBytes,
            DurationMs = durationMs,
            DumpObjectKey = dumpObjectKey,
            FileMetrics = fileMetrics,
            FileBackupError = fileError,
            PgBaseManifestKey = pgBaseManifestKey,
        };
    }

    internal async Task<(FileBackupMetrics? Metrics, string? Error)> CaptureFilesSafelyAsync(
        DatabaseConfig config,
        StorageConfig storage,
        IUploadProvider uploader,
        string backupFolder,
        string? dumpObjectKey,
        IProgressReporter<BackupStage> reporter,
        CancellationToken ct)
    {
        if (config.FilePaths.Count == 0)
            return (null, null);

        try
        {
            _logger.LogInformation(
                "Capturing file backup for database '{Database}' ({Count} path(s))",
                config.Database, config.FilePaths.Count);

            reporter.Report(BackupStage.CapturingFiles);

            var normalizedRoots = NormalizeRoots(config.FilePaths);

            await using var writer = _manifestStore.OpenWriter(
                config.Database,
                dumpObjectKey ?? string.Empty,
                roots: normalizedRoots);

            var capture = await _fileBackup.CaptureAsync(normalizedRoots, uploader, writer, reporter, ct);
            var manifestKey = await writer.CompleteAsync(uploader, backupFolder, ct);

            var metrics = new FileBackupMetrics
            {
                ManifestKey = manifestKey,
                FilesCount = checked((int)writer.FilesCount),
                FilesTotalBytes = writer.FilesTotalBytes,
                NewChunksCount = capture.NewChunksCount,
            };
            return (metrics, null);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "File backup failed for database '{Database}'", config.Database);
            return (null, "Не удалось загрузить файлы в хранилище. Подробности — в логах агента.");
        }
    }

    private async Task<(string DecryptedPath, string WorkDir)> DownloadAndDecryptManifestAsync(
        IUploadProvider uploader,
        string objectKey,
        DatabaseConfig config,
        CancellationToken ct)
    {
        var workDir = Path.Combine(config.OutputPath, $"diff-base-{Guid.NewGuid():N}");
        Directory.CreateDirectory(workDir);

        var encryptedPath = Path.Combine(workDir, "base.backup_manifest.enc");
        var decryptedPath = Path.Combine(workDir, "base.backup_manifest");

        _logger.LogInformation(
            "Downloading parent backup_manifest from storage. Key: '{Key}', Local: '{Path}'",
            objectKey, encryptedPath);

        await uploader.DownloadAsync(objectKey, encryptedPath, progress: null, ct);
        await _encryption.DecryptAsync(encryptedPath, decryptedPath, ct);

        try
        {
            if (File.Exists(encryptedPath)) File.Delete(encryptedPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete encrypted parent manifest '{Path}'", encryptedPath);
        }

        return (decryptedPath, workDir);
    }

    private void TryDelete(string? path)
    {
        if (path is null) return;
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
                _logger.LogDebug("Deleted local file '{Path}'", path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not delete local file '{Path}'", path);
        }
    }

    private void TryDeleteDirectory(string? path)
    {
        if (path is null) return;
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
                _logger.LogDebug("Deleted local directory '{Path}'", path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not delete local directory '{Path}'", path);
        }
    }

    private static IReadOnlyList<string> NormalizeRoots(IReadOnlyList<string> raw)
    {
        var result = new List<string>(raw.Count);
        foreach (var path in raw)
        {
            if (string.IsNullOrWhiteSpace(path)) continue;
            result.Add(Path.GetFullPath(path));
        }
        return result;
    }
}
