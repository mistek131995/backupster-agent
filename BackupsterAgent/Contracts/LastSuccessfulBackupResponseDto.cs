namespace BackupsterAgent.Contracts;

public sealed class LastSuccessfulBackupResponseDto
{
    public Guid Id { get; init; }
    public string? DumpObjectKey { get; init; }
    public string? PgBaseManifestKey { get; init; }
    public DateTime? BackupAt { get; init; }
}
