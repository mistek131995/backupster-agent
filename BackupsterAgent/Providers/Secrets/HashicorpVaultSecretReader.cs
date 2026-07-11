using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Secrets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Secrets;

public sealed class HashicorpVaultSecretReader : ISecretProvider, IAsyncDisposable
{
    private const string Provider = "hashicorp-vault";
    private const string DefaultKvMountPath = "secret";
    private const string TokenAuthMethod = "token";
    private const string AppRoleAuthMethod = "approle";
    private const string AppRoleAuthMethodAlias = "app-role";
    private const string RawSecretIdMode = "raw";
    private const string ResponseWrappingTokenSecretIdMode = "responsewrappingtoken";
    private static readonly TimeSpan NonExpiringTokenCacheTtl = TimeSpan.FromHours(1);

    private readonly IReadOnlyList<VaultSecretProviderConfig> _profiles;
    private readonly IHashicorpVaultSecretBackend _backend;
    private readonly SecretResolver _bootstrapSecrets;
    private readonly ILogger<HashicorpVaultSecretReader> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, CachedVaultToken> _appRoleTokens = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ITimer> _tokenRenewalTimers = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _appRoleLoginGate = new(1, 1);
    private readonly CancellationTokenSource _disposeCts = new();
    private bool _disposed;

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
        _timeProvider = TimeProvider.System;
    }

    internal HashicorpVaultSecretReader(
        IReadOnlyList<VaultSecretProviderConfig> profiles,
        IHashicorpVaultSecretBackend backend,
        TimeProvider? timeProvider = null)
    {
        _profiles = profiles;
        _backend = backend;
        _bootstrapSecrets = new SecretResolver(NullLogger<SecretResolver>.Instance);
        _logger = NullLogger<HashicorpVaultSecretReader>.Instance;
        _timeProvider = timeProvider ?? TimeProvider.System;
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
        catch (SecretResolutionException ex) when (IsAppRoleAuthMethod(profile.Auth) && IsInvalidVaultToken(ex))
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
        var cacheKey = BuildAppRoleCacheKey(profile, address, vaultNamespace);
        var now = _timeProvider.GetUtcNow();
        if (_appRoleTokens.TryGetValue(cacheKey, out var cached) && cached.RenewAtUtc > now)
            return cached.Token;

        await _appRoleLoginGate.WaitAsync(ct);
        try
        {
            now = _timeProvider.GetUtcNow();
            if (_appRoleTokens.TryGetValue(cacheKey, out cached) && cached.RenewAtUtc > now)
                return cached.Token;

            if (cached is not null && cached.Renewable && cached.ExpiresAtUtc > now)
            {
                try
                {
                    var renewed = await _backend.RenewTokenAsync(
                        address, vaultNamespace, cached.Token, settingPath, ct);
                    var renewedToken = CacheToken(
                        cacheKey, renewed, profile, address, vaultNamespace, settingPath);
                    _logger.LogDebug(
                        "HashiCorp Vault AppRole token renewed for profile {ProfileName}.",
                        profile.Name);
                    return renewedToken;
                }
                catch (SecretResolutionException ex) when (!IsVaultAuthRejected(ex))
                {
                    _logger.LogWarning(
                        ex,
                        "HashiCorp Vault AppRole token renewal failed for profile {ProfileName}; using the current token until its lease expires.",
                        profile.Name);
                    return cached.Token;
                }
                catch (SecretResolutionException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "HashiCorp Vault AppRole token renewal was rejected for profile {ProfileName}; reauthenticating.",
                        profile.Name);
                }
            }

            _appRoleTokens.TryRemove(cacheKey, out _);
            RemoveRenewalTimer(cacheKey);
            return await LoginAppRoleAndCacheAsync(
                profile, address, vaultNamespace, settingPath, cacheKey, ct);
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
        RemoveRenewalTimer(cacheKey);
    }

    private static bool IsAppRoleAuthMethod(VaultAuthConfig auth)
    {
        var method = NormalizeOptional(auth.Method)?.ToLowerInvariant() ?? TokenAuthMethod;
        return method is AppRoleAuthMethod or AppRoleAuthMethodAlias;
    }

    private static bool IsInvalidVaultToken(SecretResolutionException ex) =>
        ex.InnerException is VaultApiException { StatusCode: HttpStatusCode.Forbidden } vaultError &&
        vaultError.ContainsError("invalid token");

    private static bool IsVaultAuthRejected(SecretResolutionException ex) =>
        ex.InnerException is HttpRequestException { StatusCode: HttpStatusCode.BadRequest } ||
        IsInvalidVaultToken(ex);

    private async Task<string?> ResolveAppRoleSecretIdAsync(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath,
        CancellationToken ct)
    {
        var auth = profile.Auth;
        var secretId = await _bootstrapSecrets.ResolveOptionalStringAsync(
            auth.SecretIdSecret,
            auth.SecretId,
            $"VaultSecretProviders['{profile.Name}'].Auth.SecretId",
            ct);
        secretId = NormalizeOptional(secretId);

        var mode = NormalizeSecretIdMode(auth.SecretIdMode, settingPath);
        if (mode == RawSecretIdMode)
            return secretId;

        if (secretId is null)
            throw new SecretResolutionException(
                $"Не задан response-wrapping token HashiCorp Vault для '{settingPath}'.");

        var expectedCreationPath = NormalizeOptional(auth.SecretIdWrappingExpectedCreationPath);
        if (expectedCreationPath is null)
            throw new SecretResolutionException(
                $"Не задан ожидаемый creation_path response-wrapping token HashiCorp Vault для '{settingPath}'.");

        return await _backend.UnwrapAppRoleSecretIdAsync(
            address,
            vaultNamespace,
            secretId,
            expectedCreationPath,
            settingPath,
            ct);
    }

    private async Task<string> LoginAppRoleAndCacheAsync(
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath,
        string cacheKey,
        CancellationToken ct)
    {
        var auth = profile.Auth;
        var roleId = await ResolveBootstrapSecretAsync(
            auth.RoleIdSecret,
            auth.RoleId,
            $"VaultSecretProviders['{profile.Name}'].Auth.RoleId",
            ct);
        var secretId = await ResolveAppRoleSecretIdAsync(
            profile, address, vaultNamespace, settingPath, ct);
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

        CacheToken(cacheKey, result, profile, address, vaultNamespace, settingPath);
        _logger.LogDebug("HashiCorp Vault AppRole authentication succeeded for profile {ProfileName}.", profile.Name);
        return result.ClientToken;
    }

    private static string NormalizeSecretIdMode(string? value, string settingPath)
    {
        var mode = (NormalizeOptional(value) ?? "Raw").Replace("-", string.Empty, StringComparison.Ordinal)
            .ToLowerInvariant();
        return mode is RawSecretIdMode or ResponseWrappingTokenSecretIdMode
            ? mode
            : throw new SecretResolutionException(
                $"Режим HashiCorp Vault SecretIdMode '{value}' для '{settingPath}' не поддерживается этой версией агента.");
    }

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

    private string CacheToken(
        string cacheKey,
        VaultAppRoleLoginResult result,
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath)
    {
        var now = _timeProvider.GetUtcNow();
        var lease = result.LeaseDurationSeconds <= 0
            ? NonExpiringTokenCacheTtl
            : TimeSpan.FromSeconds(result.LeaseDurationSeconds);
        var expiresAtUtc = now.Add(lease);
        var renewAtUtc = result.Renewable
            ? expiresAtUtc - BuildRenewalSafety(result.LeaseDurationSeconds)
            : expiresAtUtc;
        _appRoleTokens[cacheKey] = new CachedVaultToken(
            result.ClientToken,
            renewAtUtc,
            expiresAtUtc,
            result.Renewable);
        ScheduleTokenRenewal(
            cacheKey, profile, address, vaultNamespace, settingPath, renewAtUtc, result.Renewable);
        return result.ClientToken;
    }

    private void ScheduleTokenRenewal(
        string cacheKey,
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath,
        DateTimeOffset renewAtUtc,
        bool renewable)
    {
        if (_disposed || !renewable)
        {
            RemoveRenewalTimer(cacheKey);
            return;
        }

        var dueTime = renewAtUtc - _timeProvider.GetUtcNow();
        if (dueTime < TimeSpan.Zero)
            dueTime = TimeSpan.Zero;

        var timer = _timeProvider.CreateTimer(
            _ => _ = RenewCachedTokenAsync(
                cacheKey, profile, address, vaultNamespace, settingPath),
            null,
            dueTime,
            Timeout.InfiniteTimeSpan);
        if (_tokenRenewalTimers.TryGetValue(cacheKey, out var previous))
            previous.Dispose();
        _tokenRenewalTimers[cacheKey] = timer;
    }

    private async Task RenewCachedTokenAsync(
        string cacheKey,
        VaultSecretProviderConfig profile,
        Uri address,
        string? vaultNamespace,
        string settingPath)
    {
        var ct = _disposeCts.Token;
        try
        {
            await _appRoleLoginGate.WaitAsync(ct);
            try
            {
                if (!_appRoleTokens.TryGetValue(cacheKey, out var cached) || !cached.Renewable)
                    return;

                var now = _timeProvider.GetUtcNow();
                if (cached.RenewAtUtc > now)
                {
                    ScheduleTokenRenewal(
                        cacheKey, profile, address, vaultNamespace, settingPath, cached.RenewAtUtc, true);
                    return;
                }

                if (cached.ExpiresAtUtc <= now)
                {
                    _appRoleTokens.TryRemove(cacheKey, out _);
                    RemoveRenewalTimer(cacheKey);
                    return;
                }

                try
                {
                    var renewed = await _backend.RenewTokenAsync(
                        address, vaultNamespace, cached.Token, settingPath, ct);
                    CacheToken(cacheKey, renewed, profile, address, vaultNamespace, settingPath);
                    _logger.LogDebug(
                        "HashiCorp Vault AppRole token renewed for profile {ProfileName}.",
                        profile.Name);
                }
                catch (SecretResolutionException ex) when (IsVaultAuthRejected(ex))
                {
                    _appRoleTokens.TryRemove(cacheKey, out _);
                    RemoveRenewalTimer(cacheKey);
                    _logger.LogWarning(
                        ex,
                        "HashiCorp Vault AppRole token renewal was rejected for profile {ProfileName}; reauthenticating.",
                        profile.Name);
                    await LoginAppRoleAndCacheAsync(
                        profile, address, vaultNamespace, settingPath, cacheKey, ct);
                }
                catch (SecretResolutionException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "HashiCorp Vault AppRole token renewal failed for profile {ProfileName}; retrying while the current lease is valid.",
                        profile.Name);
                    var remaining = cached.ExpiresAtUtc - now;
                    var retryDelay = remaining < TimeSpan.FromSeconds(10)
                        ? remaining
                        : TimeSpan.FromSeconds(10);
                    ScheduleTokenRenewal(
                        cacheKey,
                        profile,
                        address,
                        vaultNamespace,
                        settingPath,
                        now.Add(retryDelay),
                        true);
                }
            }
            finally
            {
                _appRoleLoginGate.Release();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "HashiCorp Vault AppRole token maintenance failed for profile {ProfileName}.",
                profile.Name);
        }
    }

    private void RemoveRenewalTimer(string cacheKey)
    {
        if (_tokenRenewalTimers.TryRemove(cacheKey, out var timer))
            timer.Dispose();
    }

    private static TimeSpan BuildRenewalSafety(int leaseDurationSeconds)
    {
        if (leaseDurationSeconds <= 0)
            return TimeSpan.FromMinutes(1);

        var safety = TimeSpan.FromSeconds(Math.Clamp(leaseDurationSeconds / 10, 1, 60));
        return safety;
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

    private sealed record CachedVaultToken(
        string Token,
        DateTimeOffset RenewAtUtc,
        DateTimeOffset ExpiresAtUtc,
        bool Renewable);

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _disposeCts.Cancel();
        foreach (var timer in _tokenRenewalTimers.Values)
            await timer.DisposeAsync();
        _tokenRenewalTimers.Clear();
        await _appRoleLoginGate.WaitAsync();
        _appRoleLoginGate.Release();
        _disposeCts.Dispose();
        _appRoleLoginGate.Dispose();
    }
}
