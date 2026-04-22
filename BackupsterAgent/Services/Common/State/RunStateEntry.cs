namespace BackupsterAgent.Services.Common.State;

internal sealed record RunStateEntry
{
    public required string DatabaseName { get; init; }
    public DateTime LastRunUtc { get; init; }
}
