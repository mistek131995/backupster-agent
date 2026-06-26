using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Secrets;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Services.Common.Secrets;

public sealed class SecretResolver : ISecretResolver
{
    private readonly ISecretProviderFactory _secretProviders;

    public SecretResolver(ISecretProviderFactory secretProviders)
    {
        _secretProviders = secretProviders;
    }

    internal SecretResolver(ILogger<SecretResolver> logger)
        : this(CreateDefaultProviderFactory())
    {
        _ = logger;
    }

    public async Task<string> ResolveStringAsync(
        SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct) =>
        await ResolveOptionalStringAsync(secret, plainValue, settingPath, ct) ?? string.Empty;

    public async Task<string?> ResolveOptionalStringAsync(
        SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct)
    {
        if (secret is null)
            return plainValue;

        var selection = _secretProviders.GetProvider(secret, settingPath);
        return NormalizeSecretValue(
            await selection.Provider.ReadAsync(selection.Secret, settingPath, ct),
            settingPath,
            selection.Provider.EmptyValueSourceName);
    }

    public string ResolveString(SecretRef? secret, string? plainValue, string settingPath) =>
        ResolveOptionalString(secret, plainValue, settingPath) ?? string.Empty;

    public string? ResolveOptionalString(SecretRef? secret, string? plainValue, string settingPath)
    {
        if (secret is null)
            return plainValue;

        var selection = _secretProviders.GetProvider(secret, settingPath);
        if (!selection.Provider.SupportsSynchronousReads)
        {
            var providerName = selection.Secret.Provider;
            throw new SecretResolutionException(
                $"Провайдер секретов '{providerName}' для '{settingPath}' требует асинхронного чтения.");
        }

        return NormalizeSecretValue(
            selection.Provider.Read(selection.Secret, settingPath),
            settingPath,
            selection.Provider.EmptyValueSourceName);
    }

    public bool RequiresAsyncResolution(SecretRef? secret) =>
        _secretProviders.RequiresAsyncResolution(secret);

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

    private static string NormalizeSecretValue(string raw, string settingPath, string source)
    {
        var value = raw.TrimEnd('\r', '\n');
        if (value.Length == 0)
            throw new SecretResolutionException(
                $"{source} для '{settingPath}' пустой.");

        return value;
    }

    private static ISecretProviderFactory CreateDefaultProviderFactory()
    {
        var providers = new List<ISecretProvider>
        {
            new FileSecretProvider(NullLogger<FileSecretProvider>.Instance),
            new EnvironmentSecretProvider(NullLogger<EnvironmentSecretProvider>.Instance),
        };

        return new SecretProviderFactory(providers);
    }
}
