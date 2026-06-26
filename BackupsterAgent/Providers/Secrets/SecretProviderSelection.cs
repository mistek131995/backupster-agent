using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Secrets;

public sealed record SecretProviderSelection(ISecretProvider Provider, SecretRef Secret);
