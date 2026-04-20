using BackupsterAgent.Domain;
using BackupsterAgent.Services.Upload;

namespace BackupsterAgent.Services.Backup;

public interface IManifestWriter : IAsyncDisposable
{
    long FilesCount { get; }
    long FilesTotalBytes { get; }

    Task AppendAsync(FileEntry entry, CancellationToken ct);

    Task<string> CompleteAsync(
        IUploadService uploader,
        string backupFolder,
        CancellationToken ct);
}
