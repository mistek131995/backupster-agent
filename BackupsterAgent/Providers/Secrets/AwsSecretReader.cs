using Amazon.Runtime;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class AwsSecretReader : ISecretProvider
{
    private const string SecretsManagerProvider = "aws-secrets-manager";
    private const string SsmProvider = "aws-ssm-parameter";

    private readonly IAwsSecretBackend _backend;
    private readonly ILogger<AwsSecretReader> _logger;

    public AwsSecretReader(IAwsSecretBackend backend, ILogger<AwsSecretReader> logger)
    {
        _backend = backend;
        _logger = logger;
    }

    public string EmptyValueSourceName => "AWS-секрет";
    public bool SupportsSynchronousReads => false;

    public bool CanRead(string provider) =>
        provider.Equals(SecretsManagerProvider, StringComparison.Ordinal) ||
        provider.Equals(SsmProvider, StringComparison.Ordinal);

    public async Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
    {
        if (secret.Provider == SecretsManagerProvider)
            return await ReadSecretsManagerAsync(secret, settingPath, ct);

        return await ReadSsmParameterAsync(secret, settingPath, ct);
    }

    public string Read(SecretRef secret, string settingPath)
    {
        throw new SecretResolutionException(
            $"Провайдер секретов '{secret.Provider}' для '{settingPath}' требует асинхронного чтения.");
    }

    private async Task<string> ReadSecretsManagerAsync(
        SecretRef secret,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            var value = await _backend.ReadSecretsManagerValueAsync(secret, settingPath, ct);
            return SecretJsonValueExtractor.ExtractJsonKeyIfConfigured(value, secret.JsonKey, settingPath);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is AmazonServiceException or AmazonClientException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to read AWS Secrets Manager secret for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет AWS Secrets Manager для '{settingPath}'. Проверьте имя секрета, регион, IAM-права и доступ к KMS.",
                ex);
        }
    }

    private async Task<string> ReadSsmParameterAsync(
        SecretRef secret,
        string settingPath,
        CancellationToken ct)
    {
        try
        {
            var value = await _backend.ReadSsmParameterValueAsync(secret, settingPath, ct);
            return SecretJsonValueExtractor.ExtractJsonKeyIfConfigured(value, secret.JsonKey, settingPath);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is AmazonServiceException or AmazonClientException or InvalidOperationException)
        {
            _logger.LogError(ex, "Failed to read AWS SSM parameter for {SettingPath}.", settingPath);
            throw new SecretResolutionException(
                $"Не удалось прочитать параметр AWS SSM Parameter Store для '{settingPath}'. Проверьте имя параметра, регион, IAM-права и доступ к KMS.",
                ex);
        }
    }
}
