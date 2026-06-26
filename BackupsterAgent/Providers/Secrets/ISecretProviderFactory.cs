using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Secrets;

public interface ISecretProviderFactory
{
    SecretProviderSelection GetProvider(SecretRef secret, string settingPath);
    bool RequiresAsyncResolution(SecretRef? secret);
}
