using System.Collections.Concurrent;
using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Common.Secrets;

namespace BackupsterAgent.Providers.Upload;

public sealed class UploadProviderFactory : IUploadProviderFactory, IAsyncDisposable
{
    private readonly StorageResolver _storages;
    private readonly ISecretResolver _secrets;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public UploadProviderFactory(
        StorageResolver storages,
        ISecretResolver secrets,
        ILoggerFactory loggerFactory)
    {
        _storages = storages;
        _secrets = secrets;
        _loggerFactory = loggerFactory;
    }

    public async Task<IUploadProvider> GetProviderAsync(string storageName, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageName);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var storage = _storages.Resolve(storageName);
        var resolved = await _secrets.ResolveStorageAsync(storage, ct);

        await _gate.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_cache.TryGetValue(storageName, out var cached) &&
                SameStorage(cached.Storage, resolved))
            {
                return cached.Provider;
            }

            var provider = Create(storageName, resolved);
            _cache[storageName] = new CacheEntry(resolved, provider);

            if (cached is not null)
                await DisposeProviderAsync(cached.Provider);

            return provider;
        }
        finally
        {
            _gate.Release();
        }
    }

    private IUploadProvider Create(string storageName, StorageConfig storage)
    {
        return storage.Provider switch
        {
            UploadProvider.S3 => new S3UploadProvider(
                storage.S3 ?? throw new InvalidOperationException(
                    $"Storage '{storageName}' has Provider=S3 but S3 settings are missing."),
                _loggerFactory.CreateLogger<S3UploadProvider>()),
            UploadProvider.Sftp => new SftpUploadProvider(
                storage.Sftp ?? throw new InvalidOperationException(
                    $"Storage '{storageName}' has Provider=Sftp but Sftp settings are missing."),
                _loggerFactory.CreateLogger<SftpUploadProvider>()),
            UploadProvider.AzureBlob => new AzureBlobUploadProvider(
                storage.AzureBlob ?? throw new InvalidOperationException(
                    $"Storage '{storageName}' has Provider=AzureBlob but AzureBlob settings are missing."),
                _loggerFactory.CreateLogger<AzureBlobUploadProvider>()),
            UploadProvider.WebDav => new WebDavUploadProvider(
                storage.WebDav ?? throw new InvalidOperationException(
                    $"Storage '{storageName}' has Provider=WebDav but WebDav settings are missing."),
                _loggerFactory.CreateLogger<WebDavUploadProvider>()),
            UploadProvider.LocalFs => new LocalFsUploadProvider(
                storage.LocalFs ?? throw new InvalidOperationException(
                    $"Storage '{storageName}' has Provider=LocalFs but LocalFs settings are missing."),
                _loggerFactory.CreateLogger<LocalFsUploadProvider>()),
            _ => throw new InvalidOperationException(
                $"Storage '{storageName}' has unknown provider: {storage.Provider}"),
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;

            foreach (var entry in _cache.Values)
            {
                try { await DisposeProviderAsync(entry.Provider); }
                catch { }
            }

            _cache.Clear();
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async ValueTask DisposeProviderAsync(IUploadProvider provider)
    {
        switch (provider)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    private static bool SameStorage(StorageConfig left, StorageConfig right) =>
        string.Equals(left.Name, right.Name, StringComparison.Ordinal) &&
        left.Provider == right.Provider &&
        left.Provider switch
        {
            UploadProvider.S3 => SameS3(left.S3, right.S3),
            UploadProvider.Sftp => SameSftp(left.Sftp, right.Sftp),
            UploadProvider.AzureBlob => SameAzureBlob(left.AzureBlob, right.AzureBlob),
            UploadProvider.WebDav => SameWebDav(left.WebDav, right.WebDav),
            UploadProvider.LocalFs => SameLocalFs(left.LocalFs, right.LocalFs),
            _ => true,
        };

    private static bool SameS3(S3Settings? left, S3Settings? right) =>
        left is null || right is null
            ? left is null && right is null
            : string.Equals(left.EndpointUrl, right.EndpointUrl, StringComparison.Ordinal) &&
              string.Equals(left.AccessKey, right.AccessKey, StringComparison.Ordinal) &&
              string.Equals(left.SecretKey, right.SecretKey, StringComparison.Ordinal) &&
              string.Equals(left.BucketName, right.BucketName, StringComparison.Ordinal) &&
              string.Equals(left.Region, right.Region, StringComparison.Ordinal);

    private static bool SameSftp(SftpSettings? left, SftpSettings? right) =>
        left is null || right is null
            ? left is null && right is null
            : string.Equals(left.Host, right.Host, StringComparison.Ordinal) &&
              left.Port == right.Port &&
              string.Equals(left.Username, right.Username, StringComparison.Ordinal) &&
              string.Equals(left.Password, right.Password, StringComparison.Ordinal) &&
              string.Equals(left.PrivateKeyPath, right.PrivateKeyPath, StringComparison.Ordinal) &&
              string.Equals(left.PrivateKeyPassphrase, right.PrivateKeyPassphrase, StringComparison.Ordinal) &&
              string.Equals(left.RemotePath, right.RemotePath, StringComparison.Ordinal) &&
              string.Equals(left.HostKeyFingerprint, right.HostKeyFingerprint, StringComparison.Ordinal);

    private static bool SameAzureBlob(AzureBlobSettings? left, AzureBlobSettings? right) =>
        left is null || right is null
            ? left is null && right is null
            : string.Equals(left.ConnectionString, right.ConnectionString, StringComparison.Ordinal) &&
              string.Equals(left.AccountName, right.AccountName, StringComparison.Ordinal) &&
              string.Equals(left.AccountKey, right.AccountKey, StringComparison.Ordinal) &&
              string.Equals(left.ServiceUri, right.ServiceUri, StringComparison.Ordinal) &&
              string.Equals(left.ContainerName, right.ContainerName, StringComparison.Ordinal);

    private static bool SameWebDav(WebDavSettings? left, WebDavSettings? right) =>
        left is null || right is null
            ? left is null && right is null
            : string.Equals(left.BaseUrl, right.BaseUrl, StringComparison.Ordinal) &&
              string.Equals(left.Username, right.Username, StringComparison.Ordinal) &&
              string.Equals(left.Password, right.Password, StringComparison.Ordinal) &&
              string.Equals(left.RemotePath, right.RemotePath, StringComparison.Ordinal);

    private static bool SameLocalFs(LocalFsSettings? left, LocalFsSettings? right) =>
        left is null || right is null
            ? left is null && right is null
            : string.Equals(left.RemotePath, right.RemotePath, StringComparison.Ordinal);

    private sealed record CacheEntry(StorageConfig Storage, IUploadProvider Provider);
}
