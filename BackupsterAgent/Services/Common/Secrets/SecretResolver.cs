using System.Security;
using System.Text;
using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Services.Common.Secrets;

public sealed class SecretResolver : ISecretResolver
{
    private const string FileProvider = "file";
    private const string EnvProvider = "env";
    private readonly ILogger<SecretResolver> _logger;

    public SecretResolver(ILogger<SecretResolver> logger)
    {
        _logger = logger;
    }

    public async Task<string> ResolveStringAsync(
        SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct) =>
        await ResolveOptionalStringAsync(secret, plainValue, settingPath, ct) ?? string.Empty;

    public async Task<string?> ResolveOptionalStringAsync(
        SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct)
    {
        if (secret is null)
            return plainValue;

        var provider = NormalizeProvider(secret, settingPath);
        return provider switch
        {
            FileProvider => NormalizeSecretValue(
                await ReadFileSecretAsync(RequirePath(secret, settingPath), settingPath, ct),
                settingPath,
                "Файл секрета"),
            EnvProvider => NormalizeSecretValue(
                ReadEnvironmentSecret(RequireName(secret, settingPath), settingPath),
                settingPath,
                "Переменная окружения секрета"),
            _ => throw UnsupportedProvider(provider, settingPath),
        };
    }

    public string ResolveString(SecretRef? secret, string? plainValue, string settingPath) =>
        ResolveOptionalString(secret, plainValue, settingPath) ?? string.Empty;

    public string? ResolveOptionalString(SecretRef? secret, string? plainValue, string settingPath)
    {
        if (secret is null)
            return plainValue;

        var provider = NormalizeProvider(secret, settingPath);
        return provider switch
        {
            FileProvider => NormalizeSecretValue(
                ReadFileSecret(RequirePath(secret, settingPath), settingPath),
                settingPath,
                "Файл секрета"),
            EnvProvider => NormalizeSecretValue(
                ReadEnvironmentSecret(RequireName(secret, settingPath), settingPath),
                settingPath,
                "Переменная окружения секрета"),
            _ => throw UnsupportedProvider(provider, settingPath),
        };
    }

    public async Task<ConnectionConfig> ResolveConnectionAsync(ConnectionConfig connection, CancellationToken ct) =>
        new()
        {
            Name = connection.Name,
            DatabaseType = connection.DatabaseType,
            ConnectionUri = await ResolveOptionalStringAsync(
                connection.ConnectionUriSecret,
                connection.ConnectionUri,
                $"Connections['{connection.Name}'].ConnectionUri",
                ct),
            ConnectionUriSecret = connection.ConnectionUriSecret,
            Host = connection.Host,
            Port = connection.Port,
            Username = await ResolveStringAsync(
                connection.UsernameSecret,
                connection.Username,
                $"Connections['{connection.Name}'].Username",
                ct),
            UsernameSecret = connection.UsernameSecret,
            Password = await ResolveStringAsync(
                connection.PasswordSecret,
                connection.Password,
                $"Connections['{connection.Name}'].Password",
                ct),
            PasswordSecret = connection.PasswordSecret,
            BinPath = connection.BinPath,
        };

    public async Task<StorageConfig> ResolveStorageAsync(StorageConfig storage, CancellationToken ct) =>
        storage.Provider switch
        {
            UploadProvider.S3 => new StorageConfig
            {
                Name = storage.Name,
                Provider = storage.Provider,
                S3 = storage.S3 is null ? null : await ResolveS3Async(storage.Name, storage.S3, ct),
            },
            UploadProvider.Sftp => new StorageConfig
            {
                Name = storage.Name,
                Provider = storage.Provider,
                Sftp = storage.Sftp is null ? null : await ResolveSftpAsync(storage.Name, storage.Sftp, ct),
            },
            UploadProvider.AzureBlob => new StorageConfig
            {
                Name = storage.Name,
                Provider = storage.Provider,
                AzureBlob = storage.AzureBlob is null ? null : await ResolveAzureBlobAsync(storage.Name, storage.AzureBlob, ct),
            },
            UploadProvider.WebDav => new StorageConfig
            {
                Name = storage.Name,
                Provider = storage.Provider,
                WebDav = storage.WebDav is null ? null : await ResolveWebDavAsync(storage.Name, storage.WebDav, ct),
            },
            UploadProvider.LocalFs => storage,
            _ => storage,
        };

    private async Task<S3Settings> ResolveS3Async(string storageName, S3Settings settings, CancellationToken ct) =>
        new()
        {
            EndpointUrl = settings.EndpointUrl,
            AccessKey = await ResolveStringAsync(settings.AccessKeySecret, settings.AccessKey, $"Storages['{storageName}'].S3.AccessKey", ct),
            AccessKeySecret = settings.AccessKeySecret,
            SecretKey = await ResolveStringAsync(settings.SecretKeySecret, settings.SecretKey, $"Storages['{storageName}'].S3.SecretKey", ct),
            SecretKeySecret = settings.SecretKeySecret,
            BucketName = settings.BucketName,
            Region = settings.Region,
        };

    private async Task<SftpSettings> ResolveSftpAsync(string storageName, SftpSettings settings, CancellationToken ct) =>
        new()
        {
            Host = settings.Host,
            Port = settings.Port,
            Username = await ResolveStringAsync(settings.UsernameSecret, settings.Username, $"Storages['{storageName}'].Sftp.Username", ct),
            UsernameSecret = settings.UsernameSecret,
            Password = await ResolveStringAsync(settings.PasswordSecret, settings.Password, $"Storages['{storageName}'].Sftp.Password", ct),
            PasswordSecret = settings.PasswordSecret,
            PrivateKeyPath = settings.PrivateKeyPath,
            PrivateKeyPassphrase = await ResolveStringAsync(
                settings.PrivateKeyPassphraseSecret,
                settings.PrivateKeyPassphrase,
                $"Storages['{storageName}'].Sftp.PrivateKeyPassphrase",
                ct),
            PrivateKeyPassphraseSecret = settings.PrivateKeyPassphraseSecret,
            RemotePath = settings.RemotePath,
            HostKeyFingerprint = settings.HostKeyFingerprint,
        };

    private async Task<AzureBlobSettings> ResolveAzureBlobAsync(string storageName, AzureBlobSettings settings, CancellationToken ct)
    {
        var connectionString = await ResolveOptionalStringAsync(
            settings.ConnectionStringSecret,
            settings.ConnectionString,
            $"Storages['{storageName}'].AzureBlob.ConnectionString",
            ct);

        var accountKey = string.IsNullOrWhiteSpace(connectionString)
            ? await ResolveOptionalStringAsync(
                settings.AccountKeySecret,
                settings.AccountKey,
                $"Storages['{storageName}'].AzureBlob.AccountKey",
                ct)
            : settings.AccountKey;

        return new AzureBlobSettings
        {
            ConnectionString = connectionString,
            ConnectionStringSecret = settings.ConnectionStringSecret,
            AccountName = settings.AccountName,
            AccountKey = accountKey,
            AccountKeySecret = settings.AccountKeySecret,
            ServiceUri = settings.ServiceUri,
            ContainerName = settings.ContainerName,
        };
    }

    private async Task<WebDavSettings> ResolveWebDavAsync(string storageName, WebDavSettings settings, CancellationToken ct) =>
        new()
        {
            BaseUrl = settings.BaseUrl,
            Username = await ResolveStringAsync(settings.UsernameSecret, settings.Username, $"Storages['{storageName}'].WebDav.Username", ct),
            UsernameSecret = settings.UsernameSecret,
            Password = await ResolveStringAsync(settings.PasswordSecret, settings.Password, $"Storages['{storageName}'].WebDav.Password", ct),
            PasswordSecret = settings.PasswordSecret,
            RemotePath = settings.RemotePath,
        };

    private static string NormalizeProvider(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Provider))
            throw new SecretResolutionException(
                $"Не задан провайдер секрета для '{settingPath}'. Укажите 'file' или 'env'.");

        return secret.Provider.Trim().ToLowerInvariant();
    }

    private static string RequirePath(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Path))
            throw new SecretResolutionException(
                $"Не задан путь к файлу секрета для '{settingPath}'.");

        return secret.Path;
    }

    private static string RequireName(SecretRef secret, string settingPath)
    {
        if (string.IsNullOrWhiteSpace(secret.Name))
            throw new SecretResolutionException(
                $"Не задано имя переменной окружения секрета для '{settingPath}'.");

        return secret.Name;
    }

    private async Task<string> ReadFileSecretAsync(string path, string settingPath, CancellationToken ct)
    {
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

    private string ReadFileSecret(string path, string settingPath)
    {
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

    private string ReadEnvironmentSecret(string name, string settingPath)
    {
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

    private static string NormalizeSecretValue(string raw, string settingPath, string source)
    {
        var value = raw.TrimEnd('\r', '\n');
        if (value.Length == 0)
            throw new SecretResolutionException(
                $"{source} для '{settingPath}' пустой.");

        return value;
    }

    private static SecretResolutionException UnsupportedProvider(string provider, string settingPath) =>
        new($"Провайдер секретов '{provider}' для '{settingPath}' не поддерживается этой версией агента.");
}
