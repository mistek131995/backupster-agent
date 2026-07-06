using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class EnvironmentSecretProvider : ISecretProvider
{
    private const string Provider = "env";

    public string EmptyValueSourceName => "Переменная окружения секрета";
    public bool SupportsSynchronousReads => true;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct) =>
        Task.FromResult(Read(secret, settingPath));

    public string Read(SecretRef secret, string settingPath)
    {
        RejectUnsupportedFields(secret, settingPath);
        var name = RequireName(secret, settingPath);

        var value = Environment.GetEnvironmentVariable(name);
        if (value is null)
            throw new SecretResolutionException(
                $"Переменная окружения секрета '{name}' для '{settingPath}' не задана.");

        return value;
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя переменной окружения секрета для '{settingPath}'.");

        return secret.Name.Trim();
    }

    private static void RejectUnsupportedFields(SecretRef secret, string settingPath)
    {
        var unsupportedFields = new List<string>();
        AddIfConfigured(unsupportedFields, nameof(SecretRef.Path), secret.Path);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.MountPath), secret.MountPath);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.Namespace), secret.Namespace);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.ProjectId), secret.ProjectId);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.Location), secret.Location);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.Region), secret.Region);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.ServiceUrl), secret.ServiceUrl);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.JsonKey), secret.JsonKey);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.VersionStage), secret.VersionStage);
        AddIfConfigured(unsupportedFields, nameof(SecretRef.VersionId), secret.VersionId);

        if (secret.WithDecryption is not null)
            unsupportedFields.Add(nameof(SecretRef.WithDecryption));

        if (unsupportedFields.Count == 0)
            return;

        throw new SecretResolutionException(
            $"Провайдер секретов 'env' для '{settingPath}' не поддерживает поля {string.Join(", ", unsupportedFields)}. Для переменной окружения задайте только Provider и Name.");
    }

    private static void AddIfConfigured(List<string> fields, string fieldName, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            fields.Add(fieldName);
    }
}
