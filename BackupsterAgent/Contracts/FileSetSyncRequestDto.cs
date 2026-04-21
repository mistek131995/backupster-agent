namespace BackupsterAgent.Contracts;

public sealed class FileSetSyncRequestDto
{
    public List<FileSetSyncItemDto> FileSets { get; set; } = new();
}
