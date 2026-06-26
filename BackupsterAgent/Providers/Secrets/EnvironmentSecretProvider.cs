using System.Security;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class EnvironmentSecretProvider : ISecretProvider
{
    private const string Provider = "env";
    private readonly ILogger<EnvironmentSecretProvider> _logger;

    public EnvironmentSecretProvider(ILogger<EnvironmentSecretProvider> logger)
    {
        _logger = logger;
    }

    public string EmptyValueSourceName => "Переменная окружения секрета";
    public bool SupportsSynchronousReads => true;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct) =>
        Task.FromResult(Read(secret, settingPath));

    public string Read(SecretRef secret, string settingPath)
    {
        var name = RequireName(secret, settingPath);
        try
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (value is null)
                throw new SecretResolutionException(
                    $"Переменная окружения секрета '{name}' для '{settingPath}' не задана.");

            return value;
        }
        catch (SecretResolutionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is SecurityException)
        {
            _logger.LogError(ex, "Failed to read environment secret for {SettingPath} from '{Name}'", settingPath, name);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет из переменной окружения для '{settingPath}'. Проверьте имя переменной и права доступа.",
                ex);
        }
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя переменной окружения секрета для '{settingPath}'.");

        return secret.Name;
    }
}
