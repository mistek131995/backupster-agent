namespace BackupsterAgent.Contracts;

public sealed class DatabaseSyncItemDto
{
    public string Name { get; set; } = string.Empty;
    public string DatabaseType { get; set; } = string.Empty;
}
