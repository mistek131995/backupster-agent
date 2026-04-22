using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Dashboard.Clients;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Workers;

public sealed class RetentionWorker : BackgroundService
{
    private static readonly TimeSpan StartupDelay = TimeSpan.FromMinutes(15);

    private readonly IRetentionClient _client;
    private readonly StorageResolver _storages;
    private readonly BackupDeleteService _deleter;
    private readonly RetentionSettings _settings;
    private readonly IAgentActivityLock _lock;
    private readonly ILogger<RetentionWorker> _logger;

    public RetentionWorker(
        IRetentionClient client,
        StorageResolver storages,
        BackupDeleteService deleter,
        IOptions<RetentionSettings> settings,
        IAgentActivityLock activityLock,
        ILogger<RetentionWorker> logger)
    {
        _client = client;
        _storages = storages;
        _deleter = deleter;
        _settings = settings.Value;
        _lock = activityLock;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("RetentionWorker: disabled by configuration.");
            return;
        }

        var interval = TimeSpan.FromHours(Math.Max(1, _settings.IntervalHours));
        var batchSize = Math.Max(1, _settings.BatchSize);

        _logger.LogInformation(
            "RetentionWorker started. Interval: {IntervalH}h, batch: {Batch}, first run in {StartupMin} min.",
            interval.TotalHours, batchSize, StartupDelay.TotalMinutes);

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
                using var lease = await _lock.AcquireAsync("retention", stoppingToken);
                await SweepAsync(batchSize, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RetentionWorker: unexpected error in sweep loop.");
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

        _logger.LogInformation("RetentionWorker stopped.");
    }

    private async Task SweepAsync(int batchSize, CancellationToken ct)
    {
        IReadOnlyList<ExpiredBackupRecordDto> batch;
        try
        {
            batch = await _client.GetExpiredAsync(batchSize, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Retention sweep: failed to fetch expired batch.");
            return;
        }

        if (batch.Count == 0)
        {
            _logger.LogDebug("Retention sweep: no expired records.");
            return;
        }

        var unreachable = new List<Guid>();
        int deleted = 0, failed = 0;

        foreach (var record in batch)
        {
            if (ct.IsCancellationRequested) break;

            if (string.IsNullOrWhiteSpace(record.StorageName) ||
                !_storages.TryResolve(record.StorageName, out _))
            {
                unreachable.Add(record.Id);
                continue;
            }

            var payload = new DeleteTaskPayload
            {
                StorageName = record.StorageName,
                DumpObjectKey = record.DumpObjectKey,
                ManifestKey = record.ManifestKey,
            };

            BackupDeleteResult result;
            try
            {
                result = await _deleter.RunAsync(record.Id, payload, reporter: null, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }

            if (!result.IsSuccess)
            {
                _logger.LogWarning(
                    "Retention: failed to clean up record {Id} on storage '{Storage}': {Error}. Will retry next tick.",
                    record.Id, record.StorageName, result.ErrorMessage);
                failed++;
                continue;
            }

            try
            {
                await _client.DeleteAsync(record.Id, ct);
                deleted++;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Retention: storage cleaned but dashboard DELETE failed for record {Id}. Will retry next tick.",
                    record.Id);
                failed++;
            }
        }

        if (unreachable.Count > 0)
        {
            try
            {
                await _client.MarkStorageUnreachableAsync(unreachable, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Retention: failed to mark {Count} record(s) as storage-unreachable. Will retry next tick.",
                    unreachable.Count);
            }
        }

        _logger.LogInformation(
            "Retention sweep finished: batch={Batch}, deleted={Deleted}, unreachable={Unreachable}, failed={Failed}.",
            batch.Count, deleted, unreachable.Count, failed);
    }
}
