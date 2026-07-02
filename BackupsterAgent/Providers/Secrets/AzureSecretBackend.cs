using System.Collections.Concurrent;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

internal sealed class AzureSecretBackend : IAzureSecretBackend
{
    private readonly ConcurrentDictionary<string, SecretClient> _clients = new(StringComparer.Ordinal);
    private readonly Lazy<TokenCredential> _credential = new(() => new DefaultAzureCredential());

    public async Task<string> ReadSecretValueAsync(
        Uri vaultUri,
        string secretName,
        string? version,
        string settingPath,
        CancellationToken ct)
    {
        var client = _clients.GetOrAdd(
            vaultUri.AbsoluteUri,
            uri => new SecretClient(new Uri(uri), _credential.Value));

        var response = await client.GetSecretAsync(secretName, version, ct);
        return response.Value?.Value
            ?? throw new SecretResolutionException(
                $"Azure Key Vault вернул пустой ответ для '{settingPath}'.");
    }
}
