using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Security;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers;

public sealed class ChunkGcWorker : BackgroundService
{
    private static readonly TimeSpan StartupDelay = TimeSpan.FromMinutes(10);

    private readonly StorageResolver _storages;
    private readonly IUploadProviderFactory _uploadFactory;
    private readonly EncryptionService _encryption;
    private readonly ChunkSweepService _sweep;
    private readonly GcSettings _settings;
    private readonly IAgentActivityLock _lock;
    private readonly ILogger<ChunkGcWorker> _logger;

    public ChunkGcWorker(
        StorageResolver storages,
        IUploadProviderFactory uploadFactory,
        EncryptionService encryption,
        ChunkSweepService sweep,
        IOptions<GcSettings> settings,
        IAgentActivityLock activityLock,
        ILogger<ChunkGcWorker> logger)
    {
        _storages = storages;
        _uploadFactory = uploadFactory;
        _encryption = encryption;
        _sweep = sweep;
        _settings = settings.Value;
        _lock = activityLock;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("ChunkGcWorker: disabled by configuration.");
            return;
        }

        var interval = TimeSpan.FromHours(Math.Max(1, _settings.IntervalHours));
        var grace = TimeSpan.FromHours(Math.Max(0, _settings.GraceHours));

        _logger.LogInformation(
            "ChunkGcWorker started. Interval: {IntervalH}h, grace: {GraceH}h, first run in {StartupMin} min.",
            interval.TotalHours, grace.TotalHours, StartupDelay.TotalMinutes);

        try
        {
            await Task.Delay(StartupDelay, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var lease = await _lock.AcquireAsync("chunk-gc", stoppingToken);
                await SweepAllAsync(grace, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ChunkGcWorker: unexpected error in sweep loop.");
            }

            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("ChunkGcWorker stopped.");
    }

    private async Task SweepAllAsync(TimeSpan graceWindow, CancellationToken ct)
    {
        if (!_encryption.IsConfigured)
        {
            _logger.LogWarning("ChunkGc: encryption key is not configured, sweep skipped.");
            return;
        }

        if (_storages.Names.Count == 0)
        {
            _logger.LogDebug("ChunkGc: no storages configured, nothing to sweep.");
            return;
        }

        foreach (var name in _storages.Names)
        {
            if (ct.IsCancellationRequested) break;

            var storage = _storages.Resolve(name);
            if (storage.Provider is not (UploadProvider.S3 or UploadProvider.AzureBlob or UploadProvider.LocalFs or UploadProvider.Sftp or UploadProvider.WebDav))
            {
                _logger.LogDebug(
                    "ChunkGc: skipping storage '{Storage}' — provider {Provider} has no chunk pool.",
                    name, storage.Provider);
                continue;
            }

            try
            {
                var uploader = _uploadFactory.GetProvider(name);
                await _sweep.SweepStorageAsync(uploader, name, graceWindow, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ChunkGc: sweep failed for storage '{Storage}'.", name);
            }
        }
    }
}
