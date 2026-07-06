namespace BackupsterAgent.Configuration;

public sealed class VaultSecretProviderConfig
{
    public string Name { get; init; } = string.Empty;
    public string Address { get; init; } = string.Empty;
    public string Namespace { get; init; } = string.Empty;
    public VaultAuthConfig Auth { get; init; } = new();
}
