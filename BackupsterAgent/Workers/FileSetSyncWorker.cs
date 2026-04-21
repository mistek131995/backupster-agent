using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Dashboard;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers;

public sealed class FileSetSyncWorker : BackgroundService
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);

    private readonly IFileSetSyncService _sync;
    private readonly List<FileSetConfig> _fileSets;
    private readonly ILogger<FileSetSyncWorker> _logger;

    public FileSetSyncWorker(
        IFileSetSyncService sync,
        IOptions<List<FileSetConfig>> fileSets,
        ILogger<FileSetSyncWorker> logger)
    {
        _sync = sync;
        _fileSets = fileSets.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_fileSets.Count == 0)
        {
            _logger.LogInformation("FileSetSyncWorker: no file sets configured, nothing to sync.");
            return;
        }

        _logger.LogInformation(
            "FileSetSyncWorker started. File sets to sync: {Count}", _fileSets.Count);

        var delay = InitialDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var ok = await _sync.SyncAsync(stoppingToken);
                if (ok)
                {
                    _logger.LogInformation("FileSetSyncWorker: initial sync succeeded, worker stopping.");
                    return;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FileSetSyncWorker: unexpected error during sync attempt");
            }

            _logger.LogWarning(
                "FileSetSyncWorker: sync not delivered, retrying in {DelaySec}s",
                (int)delay.TotalSeconds);

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, MaxDelay.TotalSeconds));
        }
    }
}
