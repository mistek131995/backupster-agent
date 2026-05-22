namespace BackupsterAgent.Contracts;

public sealed class OpenBackupRecordResponseDto
{
    public Guid Id { get; init; }
    public string? BaseDumpObjectKey { get; init; }
    public string? BasePgBaseManifestKey { get; init; }
}
