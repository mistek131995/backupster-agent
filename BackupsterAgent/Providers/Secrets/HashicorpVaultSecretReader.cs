using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Secrets;

public sealed class HashicorpVaultSecretReader : ISecretProvider
{
    private const string Provider = "hashicorp-vault";
    private const string DefaultKvMountPath = "secret";
    private const string TokenAuthMethod = "token";
    private const string AppRoleAuthMethod = "approle";
    private const string AppRoleAuthMethodAlias = "app-role";
    private static readonly TimeSpan NonExpiringTokenCacheTtl = TimeSpan.FromHours(1);

    private readonly IReadOnlyList<VaultSecretProviderConfig> _profiles;
    private readonly IHashicorpVaultSecretBackend _backend;
    private readonly SecretResolver _bootstrapSecrets;
    private readonly ILogger<HashicorpVaultSecretReader> _logger;
    private readonly ConcurrentDictionary<string, CachedVaultToken> _appRoleTokens = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _appRoleLoginGate = new(1, 1);

    public HashicorpVaultSecretReader(
        IOptions<List<VaultSecretProviderConfig>> profiles,
        IHashicorpVaultSecretBackend backend,
        FileSecretProvider fileSecretProvider,
        EnvironmentSecretProvider environmentSecretProvider,
        ILogger<HashicorpVaultSecretReader> logger)
    {
        _profiles = profiles.Value;
        _backend = backend;
        _bootstrapSecrets = new SecretResolver(new SecretProviderFactory([fileSecretProvider, environmentSecretProvider]));
        _logger = logger;
    }

    internal HashicorpVaultSecretReader(
        IReadOnlyList<VaultSecretProviderConfig> profiles,
        IHashicorpVaultSecretBackend backend)
    {
        _profiles = profiles;
        _backend = backend;
        _bootstrapSecrets = new SecretResolver(NullLogger<SecretResolver>.Instance);
        _logger = NullLogger<HashicorpVaultSecretReader>.Instance;
    }

    public string EmptyValueSourceName => "HashiCorp Vault-секрет";
    public bool SupportsSynchronousReads => false;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public async Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
    {
        var profile = FindProfile(secret, settingPath);
        var address = RequireAddress(profile, settingPath);
        var vaultNamespace = FirstConfigured(secret.Namespace, profile.Namespace);
        var token = await ResolveVaultTokenAsync(profile, address, vaultNamespace, settingPath, ct);
        var mountPath = NormalizePath(secret.MountPath, DefaultKvMountPath);
        var secretPath = RequireSecretPath(secret, settingPath);
        var version = NormalizeOptional(secret.VersionId);
        string dataJson;

        try
        {
            dataJson = await ReadKvV2Async(address, vaultNamespace, token, mountPath, secretPath, version, settingPath, ct);
        }
        catch (SecretResolutionException ex) when (IsAppRoleAuthMethod(profile.Auth) && IsVaultForbidden(ex))
        {
            InvalidateAppRoleToken(profile, address, vaultNamespace, token);
            _logger.LogWarning(
                "HashiCorp Vault AppRole token was rejected with 403 for profile {ProfileName}; reauthenticating once.",
                profile.Name);
            var refreshedToken = await ResolveAppRoleTokenAsync(profile, address, vaultNamespace, settingPath, ct);
            dataJson = await ReadKvV2Async(address, vaultNamespace, refreshedToken, mountPath, secretPath, version, settingPath, ct);
        }

        return ExtractVaultValue(dataJson, secret.JsonKey, settingPath);
    }

    public string Read(SecretRef secret, string settingPath)
    {
        throw new SecretResolutionException(
            $"Провайдер секретов '{secret.Provider}' для '{settingPath}' требует асинхронного чтения.");
    }

    private VaultSecretProviderConfig FindProfile(SecretRef secret, string settingPath)
    {
        var profileName = RequireSecretName(secret, settingPath);
        var profile = _profiles.FirstOrDefault(x => string.Equals(x.Name, profileName, StringComparison.Ordinal));
        return profile ?? throw new SecretResolutionException(
            $"Профиль HashiCorp Vault '{profileName}' для '{settingPath}' не найден в VaultSecretProviders[].");
    }

    private async Task<string> ResolveVaultTokenAsync(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath,
        CancellationToken ct)
    {
        var auth = profile.Auth;
        var method = NormalizeOptional(auth.Method)?.ToLowerInvariant() ?? TokenAuthMethod;
        if (method == TokenAuthMethod)
        {
            return await ResolveBootstrapSecretAsync(
                auth.TokenSecret,
                auth.Token,
                $"VaultSecretProviders['{profile.Name}'].Auth.Token",
                ct);
        }

        if (method is AppRoleAuthMethod or AppRoleAuthMethodAlias)
            return await ResolveAppRoleTokenAsync(profile, address, vaultNamespace, settingPath, ct);

        throw new SecretResolutionException(
            $"Метод аутентификации HashiCorp Vault '{auth.Method}' для '{settingPath}' не поддерживается этой версией агента.");
    }

    private async Task<string> ResolveAppRoleTokenAsync(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath,
        CancellationToken ct)
    {
        var auth = profile.Auth;
        var cacheKey = BuildAppRoleCacheKey(profile, address, vaultNamespace);
        if (_appRoleTokens.TryGetValue(cacheKey, out var cached) && cached.ExpiresAtUtc > DateTimeOffset.UtcNow)
            return cached.Token;

        await _appRoleLoginGate.WaitAsync(ct);
        try
        {
            if (_appRoleTokens.TryGetValue(cacheKey, out cached) && cached.ExpiresAtUtc > DateTimeOffset.UtcNow)
                return cached.Token;

            var roleId = await ResolveBootstrapSecretAsync(
                auth.RoleIdSecret,
                auth.RoleId,
                $"VaultSecretProviders['{profile.Name}'].Auth.RoleId",
                ct);
            var secretId = await ResolveBootstrapSecretAsync(
                auth.SecretIdSecret,
                auth.SecretId,
                $"VaultSecretProviders['{profile.Name}'].Auth.SecretId",
                ct);

            var result = await _backend.LoginAppRoleAsync(
                address,
                vaultNamespace,
                NormalizePath(auth.MountPath, "auth/approle"),
                roleId,
                secretId,
                settingPath,
                ct);

            if (string.IsNullOrWhiteSpace(result.ClientToken))
                throw new SecretResolutionException(
                    $"HashiCorp Vault не вернул client_token после AppRole-аутентификации для '{settingPath}'.");

            _appRoleTokens[cacheKey] = new CachedVaultToken(result.ClientToken, BuildTokenExpiry(result.LeaseDurationSeconds));

            _logger.LogDebug("HashiCorp Vault AppRole authentication succeeded for profile {ProfileName}.", profile.Name);
            return result.ClientToken;
        }
        finally
        {
            _appRoleLoginGate.Release();
        }
    }

    private Task<string> ReadKvV2Async(
        Uri address,
        string? vaultNamespace,
        string token,
        string mountPath,
        string secretPath,
        string? version,
        string settingPath,
        CancellationToken ct) =>
        _backend.ReadKvV2Async(
            address,
            vaultNamespace,
            token,
            mountPath,
            secretPath,
            version,
            settingPath,
            ct);

    private void InvalidateAppRoleToken(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string rejectedToken)
    {
        var cacheKey = BuildAppRoleCacheKey(profile, address, vaultNamespace);
        if (!_appRoleTokens.TryGetValue(cacheKey, out var cached) || cached.Token != rejectedToken)
            return;

        ((ICollection<KeyValuePair<string, CachedVaultToken>>)_appRoleTokens).Remove(
            new KeyValuePair<string, CachedVaultToken>(cacheKey, cached));
    }

    private static bool IsAppRoleAuthMethod(VaultAuthConfig auth)
    {
        var method = NormalizeOptional(auth.Method)?.ToLowerInvariant() ?? TokenAuthMethod;
        return method is AppRoleAuthMethod or AppRoleAuthMethodAlias;
    }

    private static bool IsVaultForbidden(SecretResolutionException ex) =>
        ex.InnerException is HttpRequestException { StatusCode: HttpStatusCode.Forbidden };

    private async Task<string> ResolveBootstrapSecretAsync(
        SecretRef? secret,
        string plainValue,
        string settingPath,
        CancellationToken ct)
    {
        var value = await _bootstrapSecrets.ResolveStringAsync(secret, plainValue, settingPath, ct);
        if (string.IsNullOrWhiteSpace(value))
            throw new SecretResolutionException($"Не задан секрет HashiCorp Vault для '{settingPath}'.");

        return value;
    }

    private static DateTimeOffset BuildTokenExpiry(int leaseDurationSeconds)
    {
        if (leaseDurationSeconds <= 0)
            return DateTimeOffset.UtcNow.Add(NonExpiringTokenCacheTtl);

        var lease = TimeSpan.FromSeconds(leaseDurationSeconds);
        var safety = TimeSpan.FromSeconds(Math.Clamp(leaseDurationSeconds / 10, 1, 60));
        return DateTimeOffset.UtcNow.Add(lease - safety);
    }

    private static string BuildAppRoleCacheKey(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace) =>
        string.Join('\u001F',
            profile.Name,
            address.AbsoluteUri,
            vaultNamespace ?? string.Empty,
            NormalizePath(profile.Auth.MountPath, "auth/approle"));

    private static Uri RequireAddress(VaultSecretProviderConfig profile, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(profile.Address))
            throw new SecretResolutionException(
                $"Не задан адрес HashiCorp Vault (Address) для профиля '{profile.Name}' при чтении '{settingPath}'.");

        if (!Uri.TryCreate(profile.Address.Trim(), UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp))
        {
            throw new SecretResolutionException(
                $"Некорректный адрес HashiCorp Vault (Address) для профиля '{profile.Name}' при чтении '{settingPath}'.");
        }

        return uri;
    }

    private static string RequireSecretName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задан профиль HashiCorp Vault (Name) для '{settingPath}'.");

        return secret.Name.Trim();
    }

    private static string RequireSecretPath(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Path))
            throw new SecretResolutionException(
                $"Не задан путь HashiCorp Vault-секрета (Path) для '{settingPath}'.");

        return secret.Path.Trim();
    }

    private static string NormalizePath(string? value, string fallback)
    {
        var path = NormalizeOptional(value) ?? fallback;
        return path.Trim('/');
    }

    private static string? NormalizeOptional(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string? FirstConfigured(string? first, string? second) =>
        NormalizeOptional(first) ?? NormalizeOptional(second);

    private static string ExtractVaultValue(string dataJson, string? jsonKey, string settingPath)
    {
        if (!string.IsNullOrWhiteSpace(jsonKey))
            return SecretJsonValueExtractor.ExtractJsonKeyIfConfigured(dataJson, jsonKey, settingPath);

        try
        {
            using var document = JsonDocument.Parse(dataJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new SecretResolutionException(
                    $"Vault-секрет для '{settingPath}' должен содержать JSON-объект.");

            JsonProperty? onlyProperty = null;
            var count = 0;
            foreach (var property in document.RootElement.EnumerateObject())
            {
                onlyProperty = property;
                count++;
                if (count > 1)
                    break;
            }

            if (count != 1)
                throw new SecretResolutionException(
                    $"Vault-секрет для '{settingPath}' содержит несколько полей. Задайте JsonKey в *Secret-настройке.");

            if (onlyProperty!.Value.Value.ValueKind != JsonValueKind.String)
                throw new SecretResolutionException(
                    $"Единственное поле Vault-секрета для '{settingPath}' должно быть строкой или задайте JsonKey.");

            return onlyProperty.Value.Value.GetString() ?? string.Empty;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (JsonException ex)
        {
            throw new SecretResolutionException(
                $"Vault-секрет для '{settingPath}' не удалось разобрать как JSON.",
                ex);
        }
    }

    private sealed record CachedVaultToken(string Token, DateTimeOffset ExpiresAtUtc);
}
