namespace BackupsterAgent.Services.Common.Outbox;

public readonly record struct PruneResult(int AgedOut, int OverCapacity)
{
    public static readonly PruneResult Empty = new(0, 0);
    public int Total => AgedOut + OverCapacity;
}
