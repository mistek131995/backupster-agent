namespace BackupsterAgent.Configuration;

public sealed class SecretRef
{
    public string Provider { get; init; } = string.Empty;
    public string? Name { get; init; }
    public string? Path { get; init; }
    public string? Region { get; init; }
    public string? ServiceUrl { get; init; }
    public string? JsonKey { get; init; }
    public string? VersionStage { get; init; }
    public string? VersionId { get; init; }
    public bool? WithDecryption { get; init; }
}
