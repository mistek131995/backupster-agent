namespace BackupsterAgent.Contracts;

public sealed class DatabaseSyncRequestDto
{
    public List<DatabaseSyncItemDto> Databases { get; set; } = new();
}
