using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Secrets;

public interface IAwsSecretBackend
{
    Task<string> ReadSecretsManagerValueAsync(SecretRef secret, string settingPath, CancellationToken ct);
    Task<string> ReadSsmParameterValueAsync(SecretRef secret, string settingPath, CancellationToken ct);
}
