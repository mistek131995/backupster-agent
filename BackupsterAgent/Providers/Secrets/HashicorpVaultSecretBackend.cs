using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

internal sealed class HashicorpVaultSecretBackend : IHashicorpVaultSecretBackend
{
    public const string HttpClientName = "HashicorpVault";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<HashicorpVaultSecretBackend> _logger;

    public HashicorpVaultSecretBackend(
        IHttpClientFactory httpClientFactory,
        ILogger<HashicorpVaultSecretBackend> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task<string> ReadKvV2Async(
        Uri address,
        string? vaultNamespace,
        string token,
        string mountPath,
        string secretPath,
        string? version,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Get,
                BuildKvV2Uri(address, mountPath, secretPath, version));
            request.Headers.Add("X-Vault-Token", token);
            AddNamespaceHeader(request, vaultNamespace);

            using var response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(request, ct);
            await ThrowIfUnsuccessfulAsync(
                response, "чтения секрета KV v2", "KV v2 read", settingPath, ct);

            var payload = await response.Content.ReadFromJsonAsync<VaultKvV2Response>(cancellationToken: ct)
                ?? throw EmptyResponse(settingPath);

            if (payload.Data?.Data.ValueKind != JsonValueKind.Object)
                throw EmptyResponse(settingPath);

            return payload.Data.Data.GetRawText();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException or NotSupportedException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to read HashiCorp Vault secret for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет HashiCorp Vault для '{settingPath}'. Проверьте адрес Vault, namespace, путь, токен и права доступа.",
                ex);
        }
    }

    public async Task<VaultAppRoleLoginResult> LoginAppRoleAsync(
        Uri address,
        string? vaultNamespace,
        string authMountPath,
        string roleId,
        string? secretId,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Post,
                BuildVaultUri(address, authMountPath, "login"));
            AddNamespaceHeader(request, vaultNamespace);
            request.Content = JsonContent.Create(new AppRoleLoginRequest(roleId, secretId));

            using var response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(request, ct);
            await ThrowIfUnsuccessfulAsync(
                response, "AppRole-аутентификации", "AppRole login", settingPath, ct);

            var payload = await response.Content.ReadFromJsonAsync<AppRoleLoginResponse>(cancellationToken: ct)
                ?? throw EmptyResponse(settingPath);

            var auth = payload.Auth;
            if (auth is null || string.IsNullOrWhiteSpace(auth.ClientToken))
                throw EmptyResponse(settingPath);

            return new VaultAppRoleLoginResult(
                auth.ClientToken,
                auth.LeaseDuration,
                auth.Renewable);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException or NotSupportedException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to authenticate to HashiCorp Vault with AppRole for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось пройти AppRole-аутентификацию в HashiCorp Vault для '{settingPath}'. Проверьте адрес Vault, namespace, role_id, secret_id и auth mount.",
                ex);
        }
    }

    public async Task<VaultAppRoleLoginResult> RenewTokenAsync(
        Uri address,
        string? vaultNamespace,
        string token,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Post,
                BuildVaultUri(address, "auth/token/renew-self"));
            request.Headers.Add("X-Vault-Token", token);
            AddNamespaceHeader(request, vaultNamespace);
            request.Content = JsonContent.Create(new { });

            using var response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(request, ct);
            await ThrowIfUnsuccessfulAsync(
                response, "обновления AppRole-токена", "AppRole token renewal", settingPath, ct);

            var payload = await response.Content.ReadFromJsonAsync<AppRoleLoginResponse>(cancellationToken: ct)
                ?? throw EmptyResponse(settingPath);
            var auth = payload.Auth;
            if (auth is null || string.IsNullOrWhiteSpace(auth.ClientToken))
                throw EmptyResponse(settingPath);

            return new VaultAppRoleLoginResult(
                auth.ClientToken,
                auth.LeaseDuration,
                auth.Renewable);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException or NotSupportedException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to renew HashiCorp Vault AppRole token for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось обновить AppRole-токен HashiCorp Vault для '{settingPath}'.",
                ex);
        }
    }

    public async Task<string> UnwrapAppRoleSecretIdAsync(
        Uri address,
        string? vaultNamespace,
        string wrappingToken,
        string expectedCreationPath,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            var lookup = await SendWrappingRequestAsync<WrappingLookupResponse>(
                address, vaultNamespace, wrappingToken, "lookup", settingPath, ct);
            var actualCreationPath = NormalizeVaultPath(lookup.Data?.CreationPath);
            if (!string.Equals(actualCreationPath, NormalizeVaultPath(expectedCreationPath), StringComparison.Ordinal))
            {
                throw new SecretResolutionException(
                    $"Response-wrapping token HashiCorp Vault для '{settingPath}' создан неожиданным путём '{actualCreationPath}'.");
            }

            var unwrapped = await SendWrappingRequestAsync<WrappedAppRoleSecretIdResponse>(
                address, vaultNamespace, wrappingToken, "unwrap", settingPath, ct);
            if (string.IsNullOrWhiteSpace(unwrapped.Data?.SecretId))
                throw EmptyResponse(settingPath);

            return unwrapped.Data.SecretId;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException or NotSupportedException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to unwrap HashiCorp Vault AppRole SecretID for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось развернуть response-wrapped SecretID HashiCorp Vault для '{settingPath}'.",
                ex);
        }
    }

    private async Task<T> SendWrappingRequestAsync<T>(
        Uri address,
        string? vaultNamespace,
        string wrappingToken,
        string operation,
        string settingPath,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            BuildVaultUri(address, "sys/wrapping", operation));
        request.Headers.Add("X-Vault-Token", wrappingToken);
        AddNamespaceHeader(request, vaultNamespace);
        request.Content = JsonContent.Create(new { });

        using var response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(request, ct);
        await ThrowIfUnsuccessfulAsync(
            response,
            $"операции response wrapping ({operation})",
            $"response wrapping {operation}",
            settingPath,
            ct);
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken: ct)
            ?? throw EmptyResponse(settingPath);
    }

    private static Uri BuildKvV2Uri(Uri address, string mountPath, string secretPath, string? version)
    {
        var uri = BuildVaultUri(address, mountPath, "data", secretPath);
        if (string.IsNullOrWhiteSpace(version))
            return uri;

        return new UriBuilder(uri)
        {
            Query = $"version={Uri.EscapeDataString(version.Trim())}"
        }.Uri;
    }

    private static Uri BuildVaultUri(Uri address, params string[] pathParts)
    {
        var encodedParts = new List<string> { "v1" };
        foreach (var part in pathParts)
        {
            foreach (var segment in SplitPath(part))
                encodedParts.Add(Uri.EscapeDataString(segment));
        }

        return new Uri(EnsureTrailingSlash(address), string.Join('/', encodedParts));
    }

    private static IEnumerable<string> SplitPath(string value) =>
        value.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static Uri EnsureTrailingSlash(Uri address)
    {
        var uri = address.AbsoluteUri;
        return uri.EndsWith("/", StringComparison.Ordinal)
            ? address
            : new Uri(uri + "/");
    }

    private static void AddNamespaceHeader(HttpRequestMessage request, string? vaultNamespace)
    {
        if (!string.IsNullOrWhiteSpace(vaultNamespace))
            request.Headers.Add("X-Vault-Namespace", vaultNamespace.Trim());
    }

    private static string NormalizeVaultPath(string? value) =>
        value?.Trim().Trim('/') ?? string.Empty;

    private static async Task ThrowIfUnsuccessfulAsync(
        HttpResponseMessage response,
        string operationRu,
        string operationEn,
        string settingPath,
        CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
            return;

        var errors = await ReadVaultErrorsAsync(response.Content, ct);
        var message =
            $"HashiCorp Vault вернул HTTP {(int)response.StatusCode} ({response.StatusCode}) во время {operationRu} для '{settingPath}'.";
        throw new SecretResolutionException(
            message,
            new VaultApiException(
                $"HashiCorp Vault returned HTTP {(int)response.StatusCode} ({response.StatusCode}) during {operationEn}.",
                response.StatusCode,
                errors));
    }

    private static async Task<IReadOnlyList<string>> ReadVaultErrorsAsync(
        HttpContent content,
        CancellationToken ct)
    {
        try
        {
            var payload = await content.ReadFromJsonAsync<VaultErrorResponse>(cancellationToken: ct);
            return payload?.Errors?
                .Where(error => !string.IsNullOrWhiteSpace(error))
                .ToArray()
                ?? [];
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is IOException or JsonException or NotSupportedException or InvalidOperationException)
        {
            return [];
        }
    }

    private static SecretResolutionException EmptyResponse(string settingPath) =>
        new($"HashiCorp Vault вернул пустой или некорректный ответ для '{settingPath}'.");

    private sealed record AppRoleLoginRequest(
        [property: JsonPropertyName("role_id")] string RoleId,
        [property: JsonPropertyName("secret_id")]
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        string? SecretId);

    private sealed class AppRoleLoginResponse
    {
        [JsonPropertyName("auth")]
        public AppRoleAuthResponse? Auth { get; init; }
    }

    private sealed class AppRoleAuthResponse
    {
        [JsonPropertyName("client_token")]
        public string ClientToken { get; init; } = string.Empty;

        [JsonPropertyName("lease_duration")]
        public int LeaseDuration { get; init; }

        [JsonPropertyName("renewable")]
        public bool Renewable { get; init; }
    }

    private sealed class VaultKvV2Response
    {
        [JsonPropertyName("data")]
        public VaultKvV2Data? Data { get; init; }
    }

    private sealed class VaultKvV2Data
    {
        [JsonPropertyName("data")]
        public JsonElement Data { get; init; }
    }

    private sealed class WrappingLookupResponse
    {
        [JsonPropertyName("data")]
        public WrappingLookupData? Data { get; init; }
    }

    private sealed class WrappingLookupData
    {
        [JsonPropertyName("creation_path")]
        public string CreationPath { get; init; } = string.Empty;
    }

    private sealed class WrappedAppRoleSecretIdResponse
    {
        [JsonPropertyName("data")]
        public WrappedAppRoleSecretIdData? Data { get; init; }
    }

    private sealed class WrappedAppRoleSecretIdData
    {
        [JsonPropertyName("secret_id")]
        public string SecretId { get; init; } = string.Empty;
    }

    private sealed class VaultErrorResponse
    {
        [JsonPropertyName("errors")]
        public List<string> Errors { get; init; } = [];
    }
}
