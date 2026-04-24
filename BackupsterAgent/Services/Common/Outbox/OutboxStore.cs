using System.Text.Json;
using System.Text.Json.Serialization;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Services.Common.Outbox;

public sealed class OutboxStore : IOutboxStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _rootDir;
    private readonly string _deadDir;
    private readonly ILogger<OutboxStore> _logger;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public OutboxStore(string rootDir, ILogger<OutboxStore> logger)
    {
        _rootDir = rootDir;
        _deadDir = Path.Combine(rootDir, "dead");
        _logger = logger;
    }

    public async Task EnqueueAsync(OutboxEntry entry, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(entry.ClientTaskId))
            throw new ArgumentException("ClientTaskId must not be empty", nameof(entry));

        await _writeLock.WaitAsync(ct);
        try
        {
            Directory.CreateDirectory(_rootDir);

            var finalPath = Path.Combine(_rootDir, $"{entry.ClientTaskId}.json");
            var tempPath = Path.Combine(_rootDir, $"{entry.ClientTaskId}.json.tmp-{Guid.NewGuid():N}");

            await using (var fs = new FileStream(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.Asynchronous))
            {
                await JsonSerializer.SerializeAsync(fs, entry, JsonOptions, ct);
                await fs.FlushAsync(ct);
                fs.Flush(true);
            }

            File.Move(tempPath, finalPath, overwrite: true);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<IReadOnlyList<OutboxEntry>> ListAsync(CancellationToken ct)
    {
        if (!Directory.Exists(_rootDir))
            return Array.Empty<OutboxEntry>();

        var entries = new List<OutboxEntry>();

        foreach (var path in Directory.EnumerateFiles(_rootDir, "*.json", SearchOption.TopDirectoryOnly))
        {
            ct.ThrowIfCancellationRequested();

            if (!path.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                await using var fs = File.OpenRead(path);
                var entry = await JsonSerializer.DeserializeAsync<OutboxEntry>(fs, JsonOptions, ct);
                if (entry is null)
                {
                    _logger.LogWarning("OutboxStore: file '{Path}' deserialized to null — skipping", path);
                    continue;
                }
                entries.Add(entry);
            }
            catch (Exception ex) when (ex is JsonException or IOException)
            {
                _logger.LogWarning(ex, "OutboxStore: failed to read outbox file '{Path}' — skipping", path);
            }
        }

        entries.Sort((a, b) => a.QueuedAt.CompareTo(b.QueuedAt));
        return entries;
    }

    public Task RemoveAsync(string clientTaskId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(clientTaskId))
            throw new ArgumentException("ClientTaskId must not be empty", nameof(clientTaskId));

        var path = Path.Combine(_rootDir, $"{clientTaskId}.json");
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "OutboxStore: failed to remove outbox file '{Path}'", path);
        }
        return Task.CompletedTask;
    }

    public Task MoveToDeadAsync(string clientTaskId, string reason, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(clientTaskId))
            throw new ArgumentException("ClientTaskId must not be empty", nameof(clientTaskId));

        var sourcePath = Path.Combine(_rootDir, $"{clientTaskId}.json");
        if (!File.Exists(sourcePath)) return Task.CompletedTask;

        Directory.CreateDirectory(_deadDir);
        var deadPath = Path.Combine(_deadDir, $"{clientTaskId}.json");
        var reasonPath = Path.Combine(_deadDir, $"{clientTaskId}.reason.txt");

        try
        {
            File.Move(sourcePath, deadPath, overwrite: true);
            File.WriteAllText(reasonPath, reason ?? string.Empty);
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "OutboxStore: failed to move '{TaskId}' to dead-letter", clientTaskId);
        }

        return Task.CompletedTask;
    }

    public async Task<PruneResult> PruneAsync(int maxEntries, int maxAgeDays, DateTime nowUtc, CancellationToken ct)
    {
        var ageEnabled = maxAgeDays > 0;
        var countEnabled = maxEntries > 0;
        if (!ageEnabled && !countEnabled) return PruneResult.Empty;

        var entries = await ListAsync(ct);
        if (entries.Count == 0) return PruneResult.Empty;

        var agedOut = 0;
        if (ageEnabled)
        {
            var cutoff = nowUtc - TimeSpan.FromDays(maxAgeDays);
            var reason = $"exceeded max age ({maxAgeDays} days)";
            foreach (var entry in entries)
            {
                if (entry.QueuedAt >= cutoff) continue;
                ct.ThrowIfCancellationRequested();
                await MoveToDeadAsync(entry.ClientTaskId, reason, ct);
                agedOut++;
            }
        }

        var overCapacity = 0;
        if (countEnabled)
        {
            var alive = agedOut > 0 ? await ListAsync(ct) : entries;
            if (alive.Count > maxEntries)
            {
                var dropCount = alive.Count - maxEntries;
                var reason = $"exceeded max entries ({maxEntries})";
                for (var i = 0; i < dropCount; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    await MoveToDeadAsync(alive[i].ClientTaskId, reason, ct);
                    overCapacity++;
                }
            }
        }

        if (agedOut > 0 || overCapacity > 0)
        {
            _logger.LogWarning(
                "OutboxStore: pruned {AgedOut} aged-out and {OverCapacity} over-capacity entries to dead-letter (limits: maxEntries={MaxEntries}, maxAgeDays={MaxAgeDays}).",
                agedOut, overCapacity, maxEntries, maxAgeDays);
        }

        return new PruneResult(agedOut, overCapacity);
    }
}
