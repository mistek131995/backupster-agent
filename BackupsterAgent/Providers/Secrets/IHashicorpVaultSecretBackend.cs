namespace BackupsterAgent.Providers.Secrets;

public interface IHashicorpVaultSecretBackend
{
    Task<string> ReadKvV2Async(
        Uri address,
        string? vaultNamespace,
        string token,
        string mountPath,
        string secretPath,
        string? version,
        string settingPath,
        CancellationToken ct);

    Task<VaultAppRoleLoginResult> LoginAppRoleAsync(
        Uri address,
        string? vaultNamespace,
        string authMountPath,
        string roleId,
        string secretId,
        string settingPath,
        CancellationToken ct);
}
