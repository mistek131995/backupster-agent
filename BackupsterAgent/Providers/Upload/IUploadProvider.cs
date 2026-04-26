namespace BackupsterAgent.Providers.Upload;

public interface IUploadProvider
{
    Task<string> UploadAsync(string filePath, string folder, IProgress<long>? progress, CancellationToken ct);

    Task UploadBytesAsync(byte[] content, string objectKey, CancellationToken ct);

    Task<bool> ExistsAsync(string objectKey, CancellationToken ct);

    Task<long> GetObjectSizeAsync(string objectKey, CancellationToken ct);

    Task DownloadAsync(string objectKey, string localPath, IProgress<long>? progress, CancellationToken ct);

    Task<byte[]> DownloadBytesAsync(string objectKey, CancellationToken ct);

    IAsyncEnumerable<StorageObject> ListAsync(string prefix, CancellationToken ct);

    Task DeleteAsync(string objectKey, CancellationToken ct);
}
