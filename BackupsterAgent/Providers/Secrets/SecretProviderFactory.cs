using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class SecretProviderFactory : ISecretProviderFactory
{
    private readonly IReadOnlyList<ISecretProvider> _providers;

    public SecretProviderFactory(IEnumerable<ISecretProvider> providers)
    {
        _providers = providers.ToArray();
    }

    public SecretProviderSelection GetProvider(SecretRef secret, string settingPath)
    {
        var provider = NormalizeProvider(secret, settingPath);
        var secretProvider = _providers.FirstOrDefault(x => x.CanRead(provider));
        return secretProvider is null
            ? throw UnsupportedProvider(provider, settingPath)
            : new SecretProviderSelection(secretProvider, WithProvider(secret, provider));
    }

    public bool RequiresAsyncResolution(SecretRef? secret)
    {
        if (string.IsNullOrWhiteSpace(secret?.Provider))
            return false;

        var provider = secret.Provider.Trim().ToLowerInvariant();
        var secretProvider = _providers.FirstOrDefault(x => x.CanRead(provider));
        return secretProvider is not null && !secretProvider.SupportsSynchronousReads;
    }

    private static SecretRef WithProvider(SecretRef secret, string provider) =>
        new()
        {
            Provider = provider,
            Name = secret.Name,
            Path = secret.Path,
            MountPath = secret.MountPath,
            Namespace = secret.Namespace,
            ProjectId = secret.ProjectId,
            Location = secret.Location,
            Region = secret.Region,
            ServiceUrl = secret.ServiceUrl,
            JsonKey = secret.JsonKey,
            VersionStage = secret.VersionStage,
            VersionId = secret.VersionId,
            WithDecryption = secret.WithDecryption,
        };

    private static string NormalizeProvider(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Provider))
            throw new SecretResolutionException(
                $"Не задан провайдер секрета для '{settingPath}'.");

        return secret.Provider.Trim().ToLowerInvariant();
    }

    private static SecretResolutionException UnsupportedProvider(string provider, string settingPath) =>
        new($"Провайдер секретов '{provider}' для '{settingPath}' не поддерживается этой версией агента.");
}
