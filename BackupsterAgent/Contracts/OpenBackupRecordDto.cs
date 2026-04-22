using BackupsterAgent.Enums;

namespace BackupsterAgent.Contracts;

public sealed class OpenBackupRecordDto
{
    public string DatabaseName { get; init; } = string.Empty;
    public string ConnectionName { get; init; } = string.Empty;
    public string StorageName { get; init; } = string.Empty;
    public DateTime? StartedAt { get; init; }
    public DatabaseType? DatabaseType { get; init; }
    public string? FileSetName { get; init; }
    public BackupMode? BackupMode { get; init; }
}
