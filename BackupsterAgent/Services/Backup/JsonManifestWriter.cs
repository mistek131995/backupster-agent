using System.IO.Compression;
using System.Text.Json;
using BackupsterAgent.Domain;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Security;

namespace BackupsterAgent.Services.Backup;

public sealed class JsonManifestWriter : IManifestWriter
{
    private const string ManifestFileName = "manifest.json.gz.enc";
    private const int FlushThresholdBytes = 64 * 1024;

    private static readonly JsonSerializerOptions EntrySerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly EncryptionService _encryption;
    private readonly ILogger<JsonManifestWriter> _logger;

    private readonly string _tempDir;
    private readonly string _gzPath;
    private readonly string _encPath;

    private FileStream? _gzFileStream;
    private GZipStream? _gzipStream;
    private Utf8JsonWriter? _jsonWriter;

    private bool _completed;
    private bool _disposed;

    public long FilesCount { get; private set; }
    public long FilesTotalBytes { get; private set; }

    public JsonManifestWriter(
        EncryptionService encryption,
        string tempDir,
        ManifestMeta meta,
        ILogger<JsonManifestWriter> logger)
    {
        _encryption = encryption;
        _logger = logger;
        _tempDir = tempDir;

        Directory.CreateDirectory(_tempDir);
        _gzPath = Path.Combine(_tempDir, "manifest.json.gz");
        _encPath = Path.Combine(_tempDir, ManifestFileName);

        _gzFileStream = new FileStream(
            _gzPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);
        _gzipStream = new GZipStream(_gzFileStream, CompressionLevel.Optimal);
        _jsonWriter = new Utf8JsonWriter(_gzipStream, new JsonWriterOptions { Indented = false });

        _jsonWriter.WriteStartObject();
        _jsonWriter.WriteNumber("schemaVersion", meta.SchemaVersion);
        _jsonWriter.WriteString("database", meta.Database);
        _jsonWriter.WriteString("dumpObjectKey", meta.DumpObjectKey);
        _jsonWriter.WriteString("createdAtUtc", meta.CreatedAtUtc);
        _jsonWriter.WriteStartArray("files");
    }

    public async Task AppendAsync(FileEntry entry, CancellationToken ct)
    {
        if (_completed) throw new InvalidOperationException("Manifest writer is already completed.");
        if (_jsonWriter is null) throw new ObjectDisposedException(nameof(JsonManifestWriter));

        JsonSerializer.Serialize(_jsonWriter, entry, EntrySerializerOptions);

        FilesCount++;
        FilesTotalBytes += entry.Size;

        if (_jsonWriter.BytesPending >= FlushThresholdBytes)
            await _jsonWriter.FlushAsync(ct);
    }

    public async Task<string> CompleteAsync(
        IUploadProvider uploader,
        string backupFolder,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(uploader);
        ArgumentException.ThrowIfNullOrWhiteSpace(backupFolder);

        if (_completed) throw new InvalidOperationException("Manifest writer is already completed.");
        if (_jsonWriter is null || _gzipStream is null || _gzFileStream is null)
            throw new ObjectDisposedException(nameof(JsonManifestWriter));

        _jsonWriter.WriteEndArray();
        _jsonWriter.WriteNumber("filesCount", FilesCount);
        _jsonWriter.WriteNumber("filesTotalBytes", FilesTotalBytes);
        _jsonWriter.WriteEndObject();

        await _jsonWriter.FlushAsync(ct);
        await _jsonWriter.DisposeAsync();
        _jsonWriter = null;

        await _gzipStream.DisposeAsync();
        _gzipStream = null;

        await _gzFileStream.DisposeAsync();
        _gzFileStream = null;

        _completed = true;

        var objectKey = $"{backupFolder.TrimEnd('/')}/{ManifestFileName}";

        await using (var gzIn = new FileStream(
            _gzPath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 65536, useAsync: true))
        await using (var encOut = new FileStream(
            _encPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true))
        {
            await _encryption.EncryptStreamAsync(gzIn, encOut, ct);
        }

        var gzSize = new FileInfo(_gzPath).Length;
        var encSize = new FileInfo(_encPath).Length;

        await uploader.UploadAsync(_encPath, backupFolder, progress: null, ct);

        _logger.LogInformation(
            "Manifest saved: {ObjectKey} (files: {Files}, totalBytes: {TotalBytes}, gz: {GzBytes} B, enc: {EncBytes} B)",
            objectKey, FilesCount, FilesTotalBytes, gzSize, encSize);

        return objectKey;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_jsonWriter is not null)
        {
            try { await _jsonWriter.DisposeAsync(); } catch { }
            _jsonWriter = null;
        }
        if (_gzipStream is not null)
        {
            try { await _gzipStream.DisposeAsync(); } catch { }
            _gzipStream = null;
        }
        if (_gzFileStream is not null)
        {
            try { await _gzFileStream.DisposeAsync(); } catch { }
            _gzFileStream = null;
        }

        TryDelete(_gzPath);
        TryDelete(_encPath);
        TryDeleteDirectoryIfEmpty(_tempDir);
    }

    private void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "JsonManifestWriter: failed to delete temp '{Path}'", path);
        }
    }

    private void TryDeleteDirectoryIfEmpty(string dir)
    {
        try
        {
            if (Directory.Exists(dir) && !Directory.EnumerateFileSystemEntries(dir).Any())
                Directory.Delete(dir);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "JsonManifestWriter: failed to cleanup temp dir '{Dir}'", dir);
        }
    }
}
