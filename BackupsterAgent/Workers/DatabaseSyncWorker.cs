using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Dashboard;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers;

public sealed class DatabaseSyncWorker : BackgroundService
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);

    private readonly IDatabaseSyncService _sync;
    private readonly List<DatabaseConfig> _databases;
    private readonly ILogger<DatabaseSyncWorker> _logger;

    public DatabaseSyncWorker(
        IDatabaseSyncService sync,
        IOptions<List<DatabaseConfig>> databases,
        ILogger<DatabaseSyncWorker> logger)
    {
        _sync = sync;
        _databases = databases.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_databases.Count == 0)
        {
            _logger.LogInformation("DatabaseSyncWorker: no databases configured, nothing to sync.");
            return;
        }

        _logger.LogInformation(
            "DatabaseSyncWorker started. Databases to sync: {Count}", _databases.Count);

        var delay = InitialDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var ok = await _sync.SyncAsync(stoppingToken);
                if (ok)
                {
                    _logger.LogInformation("DatabaseSyncWorker: initial sync succeeded, worker stopping.");
                    return;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DatabaseSyncWorker: unexpected error during sync attempt");
            }

            _logger.LogWarning(
                "DatabaseSyncWorker: sync not delivered, retrying in {DelaySec}s",
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
