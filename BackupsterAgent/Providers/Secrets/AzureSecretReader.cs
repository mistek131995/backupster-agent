using Azure;
using Azure.Identity;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class AzureSecretReader : ISecretProvider
{
    private const string Provider = "azure-key-vault";

    private readonly IAzureSecretBackend _backend;
    private readonly ILogger<AzureSecretReader> _logger;

    public AzureSecretReader(IAzureSecretBackend backend, ILogger<AzureSecretReader> logger)
    {
        _backend = backend;
        _logger = logger;
    }

    public string EmptyValueSourceName => "Azure-секрет";
    public bool SupportsSynchronousReads => false;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public async Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
    {
        var vaultUri = RequireVaultUri(secret, settingPath);
        var name = RequireName(secret, settingPath);

        try
        {
            var value = await _backend.ReadSecretValueAsync(
                vaultUri, name, NullIfWhiteSpace(secret.VersionId), settingPath, ct);
            return SecretJsonValueExtractor.ExtractJsonKeyIfConfigured(value, secret.JsonKey, settingPath);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is RequestFailedException or AuthenticationFailedException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to read Azure Key Vault secret for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет Azure Key Vault для '{settingPath}'. Проверьте адрес хранилища (ServiceUrl), имя секрета и права доступа.",
                ex);
        }
    }

    public string Read(SecretRef secret, string settingPath)
    {
        throw new SecretResolutionException(
            $"Провайдер секретов '{secret.Provider}' для '{settingPath}' требует асинхронного чтения.");
    }

    private static Uri RequireVaultUri(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.ServiceUrl))
            throw new SecretResolutionException(
                $"Не задан адрес Azure Key Vault (ServiceUrl) для '{settingPath}'.");

        if (!Uri.TryCreate(secret.ServiceUrl.Trim(), UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp))
        {
            throw new SecretResolutionException(
                $"Некорректный адрес Azure Key Vault (ServiceUrl) для '{settingPath}'.");
        }

        return uri;
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя Azure-секрета для '{settingPath}'.");

        return secret.Name.Trim();
    }

    private static string? NullIfWhiteSpace(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
