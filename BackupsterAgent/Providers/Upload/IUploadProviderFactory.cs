namespace BackupsterAgent.Providers.Upload;

public interface IUploadProviderFactory
{
    IUploadProvider GetProvider(string storageName);
}
