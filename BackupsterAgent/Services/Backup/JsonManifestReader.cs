using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.Json;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Upload;

namespace BackupsterAgent.Services.Backup;

public sealed class JsonManifestReader : IManifestReader
{
    private static readonly JsonSerializerOptions EntryOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly string _localGzPath;
    private readonly string _manifestKey;
    private readonly ILogger _logger;
    private FileStream _baseStream;
    private GZipStream _gzipStream;
    private PipeReader _pipeReader;
    private JsonReaderState _readerState;
    private bool _arrayFinished;
    private bool _disposed;

    public ManifestMeta Meta { get; }

    private JsonManifestReader(
        string localGzPath,
        string manifestKey,
        FileStream baseStream,
        GZipStream gzipStream,
        PipeReader pipeReader,
        JsonReaderState readerState,
        ManifestMeta meta,
        ILogger logger)
    {
        _localGzPath = localGzPath;
        _manifestKey = manifestKey;
        _baseStream = baseStream;
        _gzipStream = gzipStream;
        _pipeReader = pipeReader;
        _readerState = readerState;
        Meta = meta;
        _logger = logger;
    }

    public static async Task<JsonManifestReader> OpenAsync(
        string manifestKey,
        string tempDir,
        IUploadService uploader,
        EncryptionService encryption,
        ILogger logger,
        CancellationToken ct)
    {
        Directory.CreateDirectory(tempDir);
        var encPath = Path.Combine(tempDir, "manifest.json.gz.enc.dl");
        var gzPath = Path.Combine(tempDir, "manifest.json.gz");

        FileStream? baseStream = null;
        GZipStream? gzipStream = null;
        PipeReader? pipeReader = null;

        try
        {
            await uploader.DownloadAsync(manifestKey, encPath, progress: null, ct);
            await encryption.DecryptAsync(encPath, gzPath, ct);
            SafeDelete(encPath, logger);

            baseStream = new FileStream(
                gzPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 65536, useAsync: true);
            gzipStream = new GZipStream(baseStream, CompressionMode.Decompress);
            pipeReader = PipeReader.Create(gzipStream);

            var (meta, state) = await ReadMetaAndPositionAsync(pipeReader, manifestKey, ct);

            return new JsonManifestReader(gzPath, manifestKey, baseStream, gzipStream, pipeReader, state, meta, logger);
        }
        catch
        {
            if (pipeReader is not null) await pipeReader.CompleteAsync();
            if (gzipStream is not null) await gzipStream.DisposeAsync();
            if (baseStream is not null) await baseStream.DisposeAsync();
            SafeDelete(encPath, logger);
            SafeDelete(gzPath, logger);
            throw;
        }
    }

    public async IAsyncEnumerable<FileEntry> ReadFilesAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(JsonManifestReader));
        if (_arrayFinished) yield break;

        while (true)
        {
            var result = await _pipeReader.ReadAsync(ct);
            var buffer = result.Buffer;

            var parseResult = TryReadNextEntry(buffer, ref _readerState, result.IsCompleted);

            if (parseResult.NeedMoreData)
            {
                if (result.IsCompleted)
                    throw new InvalidDataException($"Manifest '{_manifestKey}' ended unexpectedly inside files array.");

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            if (parseResult.Done)
            {
                _pipeReader.AdvanceTo(parseResult.Consumed);
                _arrayFinished = true;
                yield break;
            }

            _pipeReader.AdvanceTo(parseResult.Consumed);
            yield return parseResult.Entry!;
        }
    }

    private static async Task<(ManifestMeta Meta, JsonReaderState State)> ReadMetaAndPositionAsync(
        PipeReader reader,
        string manifestKey,
        CancellationToken ct)
    {
        var state = new JsonReaderState();
        var metaBuilder = new MetaBuilder();

        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            var parseResult = TryReadMetaUntilFiles(buffer, ref state, result.IsCompleted, metaBuilder);

            if (parseResult.FilesFound)
            {
                reader.AdvanceTo(parseResult.Consumed);
                return (metaBuilder.Build(), state);
            }

            if (result.IsCompleted)
                throw new InvalidDataException($"Manifest '{manifestKey}' is truncated — 'files' array not found.");

            reader.AdvanceTo(parseResult.Consumed, buffer.End);
        }
    }

    private static MetaParseResult TryReadMetaUntilFiles(
        ReadOnlySequence<byte> buffer,
        ref JsonReaderState state,
        bool isFinal,
        MetaBuilder meta)
    {
        var reader = new Utf8JsonReader(buffer, isFinal, state);
        var lastSafeState = state;
        var lastSafePosition = buffer.Start;

        while (true)
        {
            if (!reader.Read())
            {
                state = lastSafeState;
                return new MetaParseResult(needMoreData: true, filesFound: false, consumed: lastSafePosition);
            }

            if (reader.TokenType == JsonTokenType.StartObject)
            {
                lastSafeState = reader.CurrentState;
                lastSafePosition = reader.Position;
                continue;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new InvalidDataException(
                    $"Unexpected token in manifest header: {reader.TokenType}.");
            }

            var name = reader.GetString();

            if (name == "files")
            {
                if (!reader.Read())
                {
                    state = lastSafeState;
                    return new MetaParseResult(needMoreData: true, filesFound: false, consumed: lastSafePosition);
                }

                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new InvalidDataException($"'files' must be an array, got {reader.TokenType}.");

                state = reader.CurrentState;
                return new MetaParseResult(needMoreData: false, filesFound: true, consumed: reader.Position);
            }

            if (!reader.Read())
            {
                state = lastSafeState;
                return new MetaParseResult(needMoreData: true, filesFound: false, consumed: lastSafePosition);
            }

            switch (name)
            {
                case "schemaVersion":
                    meta.SchemaVersion = reader.GetInt32();
                    break;
                case "database":
                    meta.Database = reader.GetString() ?? string.Empty;
                    break;
                case "dumpObjectKey":
                    meta.DumpObjectKey = reader.GetString() ?? string.Empty;
                    break;
                case "createdAtUtc":
                    meta.CreatedAtUtc = reader.GetDateTime();
                    break;
                default:
                    if (!reader.TrySkip())
                    {
                        state = lastSafeState;
                        return new MetaParseResult(needMoreData: true, filesFound: false, consumed: lastSafePosition);
                    }
                    break;
            }

            lastSafeState = reader.CurrentState;
            lastSafePosition = reader.Position;
        }
    }

    private static EntryParseResult TryReadNextEntry(
        ReadOnlySequence<byte> buffer,
        ref JsonReaderState state,
        bool isFinal)
    {
        var reader = new Utf8JsonReader(buffer, isFinal, state);

        if (!reader.Read())
            return EntryParseResult.NeedMore(buffer.Start);

        if (reader.TokenType == JsonTokenType.EndArray)
        {
            state = reader.CurrentState;
            return EntryParseResult.DoneAt(reader.Position);
        }

        if (reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException(
                $"Expected '{{' or ']' inside files array, got {reader.TokenType}.");

        var probe = reader;
        if (!probe.TrySkip())
            return EntryParseResult.NeedMore(buffer.Start);

        FileEntry entry;
        try
        {
            entry = JsonSerializer.Deserialize<FileEntry>(ref reader, EntryOptions)
                ?? throw new InvalidDataException("FileEntry deserialized to null.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Failed to deserialize FileEntry from manifest.", ex);
        }

        state = reader.CurrentState;
        return EntryParseResult.ReadAt(entry, reader.Position);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try { await _pipeReader.CompleteAsync(); } catch { }
        try { await _gzipStream.DisposeAsync(); } catch { }
        try { await _baseStream.DisposeAsync(); } catch { }

        SafeDelete(_localGzPath, _logger);
    }

    private static void SafeDelete(string path, ILogger logger)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "JsonManifestReader: failed to delete temp '{Path}'", path);
        }
    }

    private sealed class MetaBuilder
    {
        public int SchemaVersion { get; set; }
        public DateTime CreatedAtUtc { get; set; }
        public string Database { get; set; } = string.Empty;
        public string DumpObjectKey { get; set; } = string.Empty;

        public ManifestMeta Build() => new(SchemaVersion, CreatedAtUtc, Database, DumpObjectKey);
    }

    private readonly struct MetaParseResult
    {
        public bool NeedMoreData { get; }
        public bool FilesFound { get; }
        public SequencePosition Consumed { get; }

        public MetaParseResult(bool needMoreData, bool filesFound, SequencePosition consumed)
        {
            NeedMoreData = needMoreData;
            FilesFound = filesFound;
            Consumed = consumed;
        }
    }

    private readonly struct EntryParseResult
    {
        public bool NeedMoreData { get; }
        public bool Done { get; }
        public FileEntry? Entry { get; }
        public SequencePosition Consumed { get; }

        private EntryParseResult(bool needMoreData, bool done, FileEntry? entry, SequencePosition consumed)
        {
            NeedMoreData = needMoreData;
            Done = done;
            Entry = entry;
            Consumed = consumed;
        }

        public static EntryParseResult NeedMore(SequencePosition consumed) => new(true, false, null, consumed);
        public static EntryParseResult DoneAt(SequencePosition consumed) => new(false, true, null, consumed);
        public static EntryParseResult ReadAt(FileEntry entry, SequencePosition consumed) => new(false, false, entry, consumed);
    }
}
