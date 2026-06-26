using System.Security;
using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Secrets;

public sealed class FileSecretProvider : ISecretProvider
{
    private const string Provider = "file";
    private readonly ILogger<FileSecretProvider> _logger;

    public FileSecretProvider(ILogger<FileSecretProvider> logger)
    {
        _logger = logger;
    }

    public string EmptyValueSourceName => "Файл секрета";
    public bool SupportsSynchronousReads => true;

    public bool CanRead(string provider) =>
        provider.Equals(Provider, StringComparison.Ordinal);

    public async Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct)
    {
        var path = RequirePath(secret, settingPath);
        try
        {
            var fullPath = Path.GetFullPath(path);
            return await File.ReadAllTextAsync(fullPath, Encoding.UTF8, ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException or ArgumentException or SecurityException)
        {
            _logger.LogError(ex, "Failed to read file secret for {SettingPath} from '{Path}'", settingPath, path);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет из файла для '{settingPath}'. Проверьте путь и права доступа.", ex);
        }
    }

    public string Read(SecretRef secret, string settingPath)
    {
        var path = RequirePath(secret, settingPath);
        try
        {
            var fullPath = Path.GetFullPath(path);
            return File.ReadAllText(fullPath, Encoding.UTF8);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException or ArgumentException or SecurityException)
        {
            _logger.LogError(ex, "Failed to read file secret for {SettingPath} from '{Path}'", settingPath, path);
            throw new SecretResolutionException(
                $"Не удалось прочитать секрет из файла для '{settingPath}'. Проверьте путь и права доступа.", ex);
        }
    }

    private static string RequirePath(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Path))
            throw new SecretResolutionException(
                $"Не задан путь к файлу секрета для '{settingPath}'.");

        return secret.Path;
    }
}
