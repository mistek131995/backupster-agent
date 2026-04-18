namespace DbBackupAgent.Models;

public sealed class ConnectionSyncRequestDto
{
    public List<ConnectionSyncItemDto> Connections { get; set; } = new();
}
