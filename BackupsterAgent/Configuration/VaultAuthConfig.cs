namespace BackupsterAgent.Configuration;

public sealed class VaultAuthConfig
{
    public string Method { get; init; } = "Token";
    public string MountPath { get; init; } = "auth/approle";
    public string Token { get; init; } = string.Empty;
    public SecretRef? TokenSecret { get; init; }
    public string RoleId { get; init; } = string.Empty;
    public SecretRef? RoleIdSecret { get; init; }
    public string SecretId { get; init; } = string.Empty;
    public SecretRef? SecretIdSecret { get; init; }
}
