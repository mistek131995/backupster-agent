namespace BackupsterAgent.Domain;

public sealed record ManifestMeta(
    int SchemaVersion,
    DateTime CreatedAtUtc,
    string Database,
    string DumpObjectKey);
