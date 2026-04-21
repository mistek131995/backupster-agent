namespace BackupsterAgent.Configuration;

public sealed class FileSetConfig
{
    public string Name { get; init; } = string.Empty;
    public string StorageName { get; init; } = string.Empty;
    public List<string> Paths { get; init; } = [];
}
