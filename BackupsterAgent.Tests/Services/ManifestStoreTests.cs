using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using BackupsterAgent.Domain;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Upload;
using BackupsterAgent.Settings;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class ManifestStoreTests
{
    private static readonly JsonSerializerOptions LegacyJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private string _tempRoot = null!;
    private byte[] _key = null!;
    private EncryptionService _encryption = null!;
    private ManifestStore _store = null!;
    private FakeUploadService _uploader = null!;

    [SetUp]
    public void SetUp()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), "dbbackup-manifest-tests-" + Path.GetRandomFileName());
        Directory.CreateDirectory(_tempRoot);

        _key = RandomNumberGenerator.GetBytes(32);
        _encryption = new EncryptionService(
            Options.Create(new EncryptionSettings { Key = Convert.ToBase64String(_key) }),
            NullLogger<EncryptionService>.Instance);

        _store = new ManifestStore(
            _encryption,
            Options.Create(new RestoreSettings { TempPath = _tempRoot }),
            NullLoggerFactory.Instance,
            NullLogger<ManifestStore>.Instance);

        _uploader = new FakeUploadService();
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_tempRoot, recursive: true); } catch { }
    }

    [Test]
    public async Task RoundTrip_Empty_MetaPreservedAndNoEntries()
    {
        const string folder = "db/2026-04-20_12-00-00";

        await using (var writer = _store.OpenWriter("mydb", "dump.key"))
        {
            var key = await writer.CompleteAsync(_uploader, folder, CancellationToken.None);
            Assert.That(key, Is.EqualTo($"{folder}/manifest.json.gz.enc"));
        }

        var manifestKey = $"{folder}/manifest.json.gz.enc";
        await using var reader = await _store.OpenReaderAsync(manifestKey, _uploader, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reader.Meta.SchemaVersion, Is.EqualTo(1));
            Assert.That(reader.Meta.Database, Is.EqualTo("mydb"));
            Assert.That(reader.Meta.DumpObjectKey, Is.EqualTo("dump.key"));
        });

        var entries = await CollectAsync(reader.ReadFilesAsync(CancellationToken.None));
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task RoundTrip_ManyEntries_PreservesOrderAndFields()
    {
        const string folder = "db/2026-04-20_12-00-01";

        var sample = new List<FileEntry>();
        for (int i = 0; i < 500; i++)
        {
            var chunks = new List<string> { Hex(i), Hex(i + 1), Hex(i + 2) };
            sample.Add(new FileEntry(
                Path: $"dir{i % 10}/file-{i}.bin",
                Size: 100 + i,
                Mtime: 1700000000 + i,
                Mode: 0x1A4,
                Chunks: chunks));
        }

        await using (var writer = _store.OpenWriter("mydb", "dump.key"))
        {
            foreach (var e in sample)
                await writer.AppendAsync(e, CancellationToken.None);

            Assert.That(writer.FilesCount, Is.EqualTo(sample.Count));
            Assert.That(writer.FilesTotalBytes, Is.EqualTo(sample.Sum(x => x.Size)));

            await writer.CompleteAsync(_uploader, folder, CancellationToken.None);
        }

        var manifestKey = $"{folder}/manifest.json.gz.enc";
        await using var reader = await _store.OpenReaderAsync(manifestKey, _uploader, CancellationToken.None);

        var read = await CollectAsync(reader.ReadFilesAsync(CancellationToken.None));

        Assert.That(read, Has.Count.EqualTo(sample.Count));
        for (int i = 0; i < sample.Count; i++)
        {
            Assert.That(read[i].Path, Is.EqualTo(sample[i].Path), $"path mismatch at {i}");
            Assert.That(read[i].Size, Is.EqualTo(sample[i].Size));
            Assert.That(read[i].Mtime, Is.EqualTo(sample[i].Mtime));
            Assert.That(read[i].Mode, Is.EqualTo(sample[i].Mode));
            Assert.That(read[i].Chunks, Is.EqualTo(sample[i].Chunks));
        }
    }

    [Test]
    public async Task RoundTrip_OneFileWithManyChunks_StreamsWithoutBlowup()
    {
        const string folder = "db/2026-04-20_12-00-02";
        const int chunkCount = 10_000;

        var chunks = new List<string>(chunkCount);
        for (int i = 0; i < chunkCount; i++) chunks.Add(Hex(i));

        await using (var writer = _store.OpenWriter("mydb", "dump.key"))
        {
            await writer.AppendAsync(new FileEntry("mega.bin", 1_000_000L, 1700000000, 0, chunks), CancellationToken.None);
            await writer.CompleteAsync(_uploader, folder, CancellationToken.None);
        }

        await using var reader = await _store.OpenReaderAsync(
            $"{folder}/manifest.json.gz.enc", _uploader, CancellationToken.None);

        var read = await CollectAsync(reader.ReadFilesAsync(CancellationToken.None));
        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Chunks, Has.Count.EqualTo(chunkCount));
        Assert.That(read[0].Chunks[0], Is.EqualTo(Hex(0)));
        Assert.That(read[0].Chunks[chunkCount - 1], Is.EqualTo(Hex(chunkCount - 1)));
    }

    [Test]
    public async Task Legacy_Manifest_IsReadableViaStore()
    {
        const string folder = "legacydb/2026-01-01_00-00-00";
        var manifestKey = $"{folder}/manifest.json.enc";

        var manifest = new FileManifest(
            CreatedAtUtc: DateTime.UtcNow,
            Database: "legacydb",
            DumpObjectKey: $"{folder}/dump.enc",
            Files: [
                new FileEntry("a.bin", 10, 1700000000, 0, ["aa"]),
                new FileEntry("b.bin", 20, 1700000001, 0, ["bb", "cc"]),
            ]);

        var json = JsonSerializer.SerializeToUtf8Bytes(manifest, LegacyJsonOptions);
        var encrypted = _encryption.Encrypt(json, Encoding.UTF8.GetBytes(manifestKey));
        _uploader.StoredBytes[manifestKey] = encrypted;

        await using var reader = await _store.OpenReaderAsync(manifestKey, _uploader, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reader.Meta.Database, Is.EqualTo("legacydb"));
            Assert.That(reader.Meta.DumpObjectKey, Is.EqualTo($"{folder}/dump.enc"));
            Assert.That(reader.Meta.SchemaVersion, Is.EqualTo(0), "legacy manifests report schemaVersion=0");
        });

        var read = await CollectAsync(reader.ReadFilesAsync(CancellationToken.None));
        Assert.That(read, Has.Count.EqualTo(2));
        Assert.That(read[0].Path, Is.EqualTo("a.bin"));
        Assert.That(read[1].Chunks, Is.EqualTo(new[] { "bb", "cc" }));
    }

    [Test]
    public void OpenReader_UnknownSuffix_Throws()
    {
        Assert.ThrowsAsync<InvalidDataException>(() =>
            _store.OpenReaderAsync("folder/manifest.xyz", _uploader, CancellationToken.None));
    }

    private static async Task<List<FileEntry>> CollectAsync(IAsyncEnumerable<FileEntry> source)
    {
        var list = new List<FileEntry>();
        await foreach (var e in source) list.Add(e);
        return list;
    }

    private static string Hex(int seed)
    {
        var bytes = new byte[32];
        BitConverter.GetBytes(seed).CopyTo(bytes, 0);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private sealed class FakeUploadService : IUploadService
    {
        public Dictionary<string, byte[]> StoredBytes { get; } = new(StringComparer.Ordinal);

        public async Task<string> UploadAsync(string filePath, string folder, IProgress<long>? progress, CancellationToken ct)
        {
            var fileName = Path.GetFileName(filePath);
            var objectKey = $"{folder.TrimEnd('/')}/{fileName}";
            StoredBytes[objectKey] = await File.ReadAllBytesAsync(filePath, ct);
            return $"fake://{objectKey}";
        }

        public Task UploadBytesAsync(byte[] content, string objectKey, CancellationToken ct)
        {
            StoredBytes[objectKey] = content;
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string objectKey, CancellationToken ct) =>
            Task.FromResult(StoredBytes.ContainsKey(objectKey));

        public async Task DownloadAsync(string objectKey, string localPath, IProgress<long>? progress, CancellationToken ct)
        {
            if (!StoredBytes.TryGetValue(objectKey, out var data))
                throw new FileNotFoundException($"Fake: object '{objectKey}' not found.", objectKey);

            var dir = Path.GetDirectoryName(localPath);
            if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
            await File.WriteAllBytesAsync(localPath, data, ct);
        }

        public Task<byte[]> DownloadBytesAsync(string objectKey, CancellationToken ct)
        {
            if (!StoredBytes.TryGetValue(objectKey, out var data))
                throw new FileNotFoundException($"Fake: object '{objectKey}' not found.", objectKey);
            return Task.FromResult(data);
        }

        public async IAsyncEnumerable<StorageObject> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var kvp in StoredBytes)
            {
                if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
                    yield return new StorageObject(kvp.Key, DateTime.UtcNow, kvp.Value.Length);
            }
            await Task.CompletedTask;
        }

        public Task DeleteAsync(string objectKey, CancellationToken ct)
        {
            StoredBytes.Remove(objectKey);
            return Task.CompletedTask;
        }
    }
}
