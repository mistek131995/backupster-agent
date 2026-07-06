namespace BackupsterAgent.Providers.Secrets;

public interface IGoogleSecretManagerSecretBackend
{
    Task<string> ReadSecretVersionAsync(
        string secretVersionName,
        string? location,
        string settingPath,
        CancellationToken ct);
}
