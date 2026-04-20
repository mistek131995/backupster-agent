using BackupsterAgent.Domain;

namespace BackupsterAgent.Services.Backup;

public interface IManifestReader : IAsyncDisposable
{
    ManifestMeta Meta { get; }

    IAsyncEnumerable<FileEntry> ReadFilesAsync(CancellationToken ct);
}
