namespace BackupsterAgent.Configuration;

public sealed class SecretRef
{
    public string Provider { get; init; } = string.Empty;
    public string? Name { get; init; }
    public string? Path { get; init; }
}
