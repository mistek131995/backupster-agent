using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Upload;

namespace BackupsterAgent.Services.Backup;

public sealed class LegacyJsonManifestReader : IManifestReader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly FileManifest _manifest;
    private bool _disposed;

    public ManifestMeta Meta { get; }

    private LegacyJsonManifestReader(FileManifest manifest)
    {
        _manifest = manifest;
        Meta = new ManifestMeta(
            SchemaVersion: 0,
            CreatedAtUtc: manifest.CreatedAtUtc,
            Database: manifest.Database,
            DumpObjectKey: manifest.DumpObjectKey);
    }

    public static async Task<LegacyJsonManifestReader> OpenAsync(
        string manifestKey,
        IUploadService uploader,
        EncryptionService encryption,
        CancellationToken ct)
    {
        var encrypted = await uploader.DownloadBytesAsync(manifestKey, ct);
        var aad = Encoding.UTF8.GetBytes(manifestKey);
        var json = encryption.Decrypt(encrypted, aad);

        var manifest = JsonSerializer.Deserialize<FileManifest>(json, JsonOptions)
            ?? throw new InvalidDataException($"Legacy manifest '{manifestKey}' deserialized to null.");

        return new LegacyJsonManifestReader(manifest);
    }

    public async IAsyncEnumerable<FileEntry> ReadFilesAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(LegacyJsonManifestReader));
        foreach (var entry in _manifest.Files)
        {
            ct.ThrowIfCancellationRequested();
            yield return entry;
        }
        await Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
