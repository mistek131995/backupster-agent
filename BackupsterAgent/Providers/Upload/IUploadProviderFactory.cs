namespace BackupsterAgent.Providers.Upload;

public interface IUploadProviderFactory
{
    Task<IUploadProvider> GetProviderAsync(string storageName, CancellationToken ct);
}
