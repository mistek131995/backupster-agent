namespace BackupsterAgent.Domain;

public sealed class DifferentialBackupContext
{
    public required Guid BaseBackupRecordId { get; init; }
    public string? BaseDumpObjectKey { get; init; }
    public string? BasePgBaseManifestPath { get; init; }
    public DateTime? BaseBackupAt { get; init; }
}
