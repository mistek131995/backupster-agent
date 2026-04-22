namespace BackupsterAgent.Providers.Upload;

public sealed record StorageObject(string Key, DateTime LastModifiedUtc, long Size);
