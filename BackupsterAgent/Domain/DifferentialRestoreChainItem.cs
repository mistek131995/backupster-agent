using BackupsterAgent.Enums;

namespace BackupsterAgent.Domain;

public sealed class DifferentialRestoreChainItem
{
    public required Guid BackupRecordId { get; init; }
    public required string DumpFilePath { get; init; }
    public required BackupMode BackupMode { get; init; }
    public string? PgBaseManifestFilePath { get; init; }
}
