using BackupsterAgent.Enums;

namespace BackupsterAgent.Contracts;

public sealed class RestoreChainItemDto
{
    public Guid BackupRecordId { get; init; }
    public string DumpObjectKey { get; init; } = string.Empty;
    public BackupMode BackupMode { get; init; } = BackupMode.Physical;
    public string? PgBaseManifestKey { get; init; }
}
