using System.Collections.Concurrent;
using BackupsterAgent.Exceptions;
using Google.Cloud.SecretManager.V1;

namespace BackupsterAgent.Providers.Secrets;

internal sealed class GoogleSecretManagerSecretBackend : IGoogleSecretManagerSecretBackend
{
    private readonly ConcurrentDictionary<string, Lazy<Task<SecretManagerServiceClient>>> _clients = new(StringComparer.Ordinal);
    private readonly Func<string?, Task<SecretManagerServiceClient>> _clientFactory;

    public GoogleSecretManagerSecretBackend()
        : this(CreateClientAsync)
    {
    }

    internal GoogleSecretManagerSecretBackend(Func<string?, Task<SecretManagerServiceClient>> clientFactory)
    {
        ArgumentNullException.ThrowIfNull(clientFactory);

        _clientFactory = clientFactory;
    }

    public async Task<string> ReadSecretVersionAsync(
        string secretVersionName,
        string? location,
        string settingPath,
        CancellationToken ct)
    {
        var client = await GetClientAsync(location, ct);
        var response = await client.AccessSecretVersionAsync(secretVersionName, ct);

        return response.Payload?.Data.ToStringUtf8()
            ?? throw new SecretResolutionException(
                $"Google Secret Manager вернул пустой ответ для '{settingPath}'.");
    }

    internal async Task<SecretManagerServiceClient> GetClientAsync(string? location, CancellationToken ct)
    {
        var key = BuildClientCacheKey(location);
        var client = _clients.GetOrAdd(
            key,
            _ => new Lazy<Task<SecretManagerServiceClient>>(
                () => _clientFactory(location),
                LazyThreadSafetyMode.ExecutionAndPublication));
        Task<SecretManagerServiceClient>? clientTask = null;

        try
        {
            clientTask = client.Value;
            return await clientTask.WaitAsync(ct);
        }
        catch
        {
            if (clientTask is null || clientTask.IsFaulted || clientTask.IsCanceled)
            {
                RemoveClient(key, client);
            }

            throw;
        }
    }

    private static Task<SecretManagerServiceClient> CreateClientAsync(string? location)
    {
        var endpoint = BuildEndpoint(location);
        return endpoint is null
            ? SecretManagerServiceClient.CreateAsync()
            : new SecretManagerServiceClientBuilder { Endpoint = endpoint }.BuildAsync();
    }

    internal static string? BuildEndpoint(string? location)
    {
        var normalized = NormalizeLocation(location);
        return normalized is null
            ? null
            : $"secretmanager.{normalized}.rep.googleapis.com";
    }

    private static string BuildClientCacheKey(string? location) =>
        NormalizeLocation(location) ?? string.Empty;

    private static string? NormalizeLocation(string? location) =>
        string.IsNullOrWhiteSpace(location) ? null : location.Trim().ToLowerInvariant();

    private void RemoveClient(string key, Lazy<Task<SecretManagerServiceClient>> client)
    {
        var clients = (ICollection<KeyValuePair<string, Lazy<Task<SecretManagerServiceClient>>>>)_clients;
        clients.Remove(new KeyValuePair<string, Lazy<Task<SecretManagerServiceClient>>>(key, client));
    }
}
