using BackupsterAgent.Domain;

namespace BackupsterAgent.Services.Common.Outbox;

public interface IOutboxStore
{
    Task EnqueueAsync(OutboxEntry entry, CancellationToken ct);
    Task<IReadOnlyList<OutboxEntry>> ListAsync(CancellationToken ct);
    Task RemoveAsync(string clientTaskId, CancellationToken ct);
    Task MoveToDeadAsync(string clientTaskId, string reason, CancellationToken ct);
    Task<PruneResult> PruneAsync(int maxEntries, int maxAgeDays, DateTime nowUtc, CancellationToken ct);
}
