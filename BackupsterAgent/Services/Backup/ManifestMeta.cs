namespace BackupsterAgent.Services.Backup;

public sealed record ManifestMeta(
    int SchemaVersion,
    DateTime CreatedAtUtc,
    string Database,
    string DumpObjectKey);
