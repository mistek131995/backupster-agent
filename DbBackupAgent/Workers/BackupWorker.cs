using DbBackupAgent.Models;
using DbBackupAgent.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DbBackupAgent.Workers;

public sealed class BackupWorker : BackgroundService
{
    private readonly BackupJob _job;
    private readonly ScheduleService _schedule;
    private readonly EncryptionService _encryption;
    private readonly List<DatabaseConfig> _databases;
    private readonly ILogger<BackupWorker> _logger;

    public BackupWorker(
        BackupJob job,
        ScheduleService schedule,
        EncryptionService encryption,
        IOptions<List<DatabaseConfig>> databases,
        ILogger<BackupWorker> logger)
    {
        _job = job;
        _schedule = schedule;
        _encryption = encryption;
        _databases = databases.Value;
        _logger = logger;
    }

    private bool IsConfigured()
    {
        if (_databases.Count == 0)
        {
            _logger.LogWarning("BackupWorker: no databases configured. Fill in appsettings.json and restart.");
            return false;
        }

        if (!_encryption.IsConfigured)
        {
            _logger.LogWarning("BackupWorker: encryption key is not configured. Fill in appsettings.json and restart.");
            return false;
        }

        return true;
    }

    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "BackupWorker started. Databases: {Count}, tick: {TickSec}s, schedule poll: {PollMin} min",
            _databases.Count, TickInterval.TotalSeconds, ScheduleService.PollInterval.TotalMinutes);

        var lastRunByDb = new Dictionary<string, DateTime?>(StringComparer.Ordinal);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var due = new List<(DatabaseConfig config, DateTime nextRun)>();

                foreach (var config in _databases)
                {
                    if (stoppingToken.IsCancellationRequested) break;

                    var nextRun = await _schedule.GetNextRunAsync(config.Database, stoppingToken);

                    if (nextRun is null)
                    {
                        _logger.LogDebug(
                            "BackupWorker: schedule is inactive for '{Database}', skipping", config.Database);
                        continue;
                    }

                    var last = lastRunByDb.GetValueOrDefault(config.Database);
                    if (nextRun.Value <= DateTime.UtcNow && (last is null || nextRun.Value > last))
                    {
                        due.Add((config, nextRun.Value));
                        lastRunByDb[config.Database] = nextRun.Value;
                    }
                    else
                    {
                        _logger.LogDebug(
                            "BackupWorker: '{Database}' next run at {NextRun:u}, nothing to do yet",
                            config.Database, nextRun.Value);
                    }
                }

                if (due.Count > 0 && IsConfigured())
                    await RunDueDatabasesAsync(due, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BackupWorker: unexpected error in schedule loop");
            }

            try
            {
                await Task.Delay(TickInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("BackupWorker stopped");
    }

    private async Task RunDueDatabasesAsync(
        List<(DatabaseConfig config, DateTime nextRun)> due,
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "BackupWorker: starting backup run for {Count} due database(s)", due.Count);

        int succeeded = 0;
        int failed = 0;

        for (int i = 0; i < due.Count; i++)
        {
            if (stoppingToken.IsCancellationRequested) break;

            var (config, nextRun) = due[i];

            _logger.LogInformation(
                "[{Index}/{Total}] Starting backup. Database: '{Database}', Type: {DatabaseType}, NextRun: {NextRun:u}",
                i + 1, due.Count, config.Database, config.DatabaseType, nextRun);

            try
            {
                var result = await _job.RunAsync(config, stoppingToken);

                if (result.Success)
                {
                    succeeded++;
                    _logger.LogInformation(
                        "[{Index}/{Total}] Backup succeeded. Database: '{Database}'",
                        i + 1, due.Count, config.Database);
                }
                else
                {
                    failed++;
                    _logger.LogError(
                        "[{Index}/{Total}] Backup failed. Database: '{Database}', Error: {ErrorMessage}",
                        i + 1, due.Count, config.Database, result.ErrorMessage);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("BackupWorker cancelled during '{Database}'", config.Database);
                return;
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogError(ex,
                    "[{Index}/{Total}] Unhandled exception for database '{Database}'. Continuing.",
                    i + 1, due.Count, config.Database);
            }
        }

        _logger.LogInformation(
            "BackupWorker: run complete. Succeeded: {Succeeded}, Failed: {Failed}, Total: {Total}",
            succeeded, failed, due.Count);
    }
}
