namespace BackupsterAgent.Services.Backup;

public sealed record ChunkSweepResult
{
    public int ManifestCount { get; init; }
    public int ReferencedChunks { get; init; }
    public int TotalChunks { get; init; }
    public int Deleted { get; init; }
    public int SkippedGrace { get; init; }
    public long FreedBytes { get; init; }
}
