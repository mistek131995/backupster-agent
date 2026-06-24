namespace BackupsterAgent.Configuration;

public sealed class SecretRef
{
    public string Provider { get; init; } = string.Empty;
    public string? Path { get; init; }
}
