using System.Collections.Concurrent;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

internal sealed class AwsSecretBackend : IAwsSecretBackend, IDisposable
{
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private readonly ConcurrentDictionary<string, Lazy<IAmazonSecretsManager>> _secretsClients = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Lazy<IAmazonSimpleSystemsManagement>> _ssmClients = new(StringComparer.Ordinal);

    public async Task<string> ReadSecretsManagerValueAsync(
        SecretRef secret,
        string settingPath,
        CancellationToken ct)
    {
        var secretId = RequireName(secret, settingPath);
        var client = _secretsClients.GetOrAdd(
            BuildClientCacheKey(secret),
            _ => new Lazy<IAmazonSecretsManager>(
                () => CreateSecretsManagerClient(secret),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;

        var response = await client.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = secretId,
            VersionStage = NullIfWhiteSpace(secret.VersionStage),
            VersionId = NullIfWhiteSpace(secret.VersionId),
        }, ct);

        return response.SecretString is not null
            ? response.SecretString
            : await ReadBinarySecretAsync(response.SecretBinary, settingPath, ct);
    }

    public async Task<string> ReadSsmParameterValueAsync(
        SecretRef secret,
        string settingPath,
        CancellationToken ct)
    {
        var name = RequireName(secret, settingPath);
        var client = _ssmClients.GetOrAdd(
            BuildClientCacheKey(secret),
            _ => new Lazy<IAmazonSimpleSystemsManagement>(
                () => CreateSsmClient(secret),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;

        var response = await client.GetParameterAsync(new GetParameterRequest
        {
            Name = name,
            WithDecryption = secret.WithDecryption ?? true,
        }, ct);

        if (response.Parameter?.Value is null)
            throw new SecretResolutionException(
                $"AWS SSM Parameter Store вернул пустой ответ для '{settingPath}'.");

        return response.Parameter.Value;
    }

    private static async Task<string> ReadBinarySecretAsync(Stream? stream, string settingPath, CancellationToken ct)
    {
        if (stream is null)
            throw new SecretResolutionException(
                $"AWS Secrets Manager не вернул SecretString или SecretBinary для '{settingPath}'.");

        using var memory = new MemoryStream();
        await stream.CopyToAsync(memory, ct);

        try
        {
            return StrictUtf8.GetString(memory.ToArray());
        }
        catch (DecoderFallbackException ex)
        {
            throw new SecretResolutionException(
                $"AWS Secrets Manager SecretBinary для '{settingPath}' не является UTF-8 текстом. Сохраните секрет как SecretString или используйте текстовое UTF-8 значение.",
                ex);
        }
    }

    private static IAmazonSecretsManager CreateSecretsManagerClient(SecretRef secret)
    {
        var config = new AmazonSecretsManagerConfig();
        ApplyAwsClientConfig(config, secret);
        return new AmazonSecretsManagerClient(config);
    }

    private static IAmazonSimpleSystemsManagement CreateSsmClient(SecretRef secret)
    {
        var config = new AmazonSimpleSystemsManagementConfig();
        ApplyAwsClientConfig(config, secret);
        return new AmazonSimpleSystemsManagementClient(config);
    }

    private static void ApplyAwsClientConfig(ClientConfig config, SecretRef secret)
    {
        var serviceUrl = NullIfWhiteSpace(secret.ServiceUrl);
        var region = NullIfWhiteSpace(secret.Region);

        if (region is not null)
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(region);

        if (serviceUrl is not null)
        {
            config.ServiceURL = serviceUrl;

            if (region is not null)
                config.AuthenticationRegion = region;
        }
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя AWS-секрета для '{settingPath}'.");

        return secret.Name.Trim();
    }

    private static string BuildClientCacheKey(SecretRef secret) =>
        string.Join('\u001F',
            secret.Region?.Trim().ToLowerInvariant() ?? string.Empty,
            secret.ServiceUrl?.Trim() ?? string.Empty);

    private static string? NullIfWhiteSpace(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    public void Dispose()
    {
        foreach (var client in _secretsClients.Values)
        {
            if (client.IsValueCreated)
                client.Value.Dispose();
        }

        foreach (var client in _ssmClients.Values)
        {
            if (client.IsValueCreated)
                client.Value.Dispose();
        }
    }
}
