namespace BackupsterAgent.Providers.Secrets;

public interface IAzureSecretBackend
{
    Task<string> ReadSecretValueAsync(
        Uri vaultUri,
        string secretName,
        string? version,
        string settingPath,
        CancellationToken ct);
}
