using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using Grpc.Core;

namespace BackupsterAgent.Providers.Secrets;

public sealed class GoogleSecretManagerSecretReader : ISecretProvider
{
    private const string Provider = "google-secret-manager";
    private const string LatestVersion = "latest";

    private readonly IGoogleSecretManagerSecretBackend _backend;
    private readonly ILogger<GoogleSecretManagerSecretReader> _logger;

    public GoogleSecretManagerSecretReader(
        IGoogleSecretManagerSecretBackend backend,
        ILogger<GoogleSecretManagerSecretReader> logger)
    {
        _backend = backend;
        _logger = logger;
    }

    public string EmptyValueSourceName => "Google Secret Manager-секрет";
    public bool SupportsSynchronousReads => false;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public async Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
    {
        var secretVersion = BuildSecretVersionName(secret, settingPath);

        string value;
        try
        {
            value = await _backend.ReadSecretVersionAsync(
                secretVersion.Name,
                secretVersion.Location,
                settingPath,
                ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is RpcException or InvalidOperationException or ArgumentException)
        {
            _logger.LogError(ex, "Failed to read Google Secret Manager secret for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет Google Secret Manager для '{settingPath}'. Проверьте проект, имя секрета, версию, ADC-аутентификацию и права доступа.",
                ex);
        }

        return SecretJsonValueExtractor.ExtractJsonKeyIfConfigured(value, secret.JsonKey, settingPath);
    }

    public string Read(SecretRef secret, string settingPath)
    {
        throw new SecretResolutionException(
            $"Провайдер секретов '{secret.Provider}' для '{settingPath}' требует асинхронного чтения.");
    }

    private static SecretVersionReference BuildSecretVersionName(SecretRef secret, string settingPath)
    {
        var name = RequireName(secret, settingPath);
        var version = NormalizeOptional(secret.VersionId) ?? LatestVersion;

        if (IsFullSecretVersionName(name))
            return new SecretVersionReference(name, ExtractLocation(name));

        if (IsSecretResourceName(name))
            return new SecretVersionReference($"{name}/versions/{version}", ExtractLocation(name));

        var projectId = RequireProjectId(secret, settingPath);
        var location = NormalizeOptional(secret.Location);
        return location is null
            ? new SecretVersionReference($"projects/{projectId}/secrets/{name}/versions/{version}", null)
            : new SecretVersionReference(
                $"projects/{projectId}/locations/{location}/secrets/{name}/versions/{version}",
                location);
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя Google Secret Manager-секрета для '{settingPath}'.");

        return secret.Name.Trim().Trim('/');
    }

    private static string RequireProjectId(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.ProjectId))
            throw new SecretResolutionException(
                $"Не задан ProjectId Google Secret Manager для '{settingPath}'.");

        return secret.ProjectId.Trim();
    }

    private static bool IsFullSecretVersionName(string name) =>
        name.StartsWith("projects/", StringComparison.Ordinal) &&
        name.Contains("/secrets/", StringComparison.Ordinal) &&
        name.Contains("/versions/", StringComparison.Ordinal);

    private static bool IsSecretResourceName(string name) =>
        name.StartsWith("projects/", StringComparison.Ordinal) &&
        name.Contains("/secrets/", StringComparison.Ordinal);

    private static string? NormalizeOptional(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string? ExtractLocation(string name)
    {
        const string marker = "/locations/";
        var markerIndex = name.IndexOf(marker, StringComparison.Ordinal);
        if (markerIndex < 0)
            return null;

        var start = markerIndex + marker.Length;
        var end = name.IndexOf('/', start);
        return end > start ? name[start..end] : null;
    }

    private sealed record SecretVersionReference(string Name, string? Location);
}
