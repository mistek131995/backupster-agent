using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Security;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Services.Backup;

public sealed class ManifestStore
{
    internal const string LegacySuffix = "/manifest.json.enc";
    internal const string NewSuffix = "/manifest.json.gz.enc";

    private readonly EncryptionService _encryption;
    private readonly RestoreSettings _restoreSettings;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ManifestStore> _logger;

    public ManifestStore(
        EncryptionService encryption,
        IOptions<RestoreSettings> restoreSettings,
        ILoggerFactory loggerFactory,
        ILogger<ManifestStore> logger)
    {
        _encryption = encryption;
        _restoreSettings = restoreSettings.Value;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public IManifestWriter OpenWriter(string database, string dumpObjectKey)
    {
        var tempDir = BuildWriterTempDir();
        var meta = new ManifestMeta(
            SchemaVersion: 1,
            CreatedAtUtc: DateTime.UtcNow,
            Database: database,
            DumpObjectKey: dumpObjectKey);

        return new JsonManifestWriter(
            _encryption,
            tempDir,
            meta,
            _loggerFactory.CreateLogger<JsonManifestWriter>());
    }

    public Task<IManifestReader> OpenReaderAsync(
        string manifestKey,
        IUploadProvider uploader,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestKey);

        if (manifestKey.EndsWith(NewSuffix, StringComparison.Ordinal))
            return OpenNewAsync(manifestKey, uploader, ct);

        if (manifestKey.EndsWith(LegacySuffix, StringComparison.Ordinal))
            return OpenLegacyAsync(manifestKey, uploader, ct);

        throw new InvalidDataException(
            $"Unknown manifest suffix for '{manifestKey}'. Expected '{NewSuffix}' or '{LegacySuffix}'.");
    }

    private async Task<IManifestReader> OpenNewAsync(
        string manifestKey, IUploadProvider uploader, CancellationToken ct)
    {
        var tempDir = BuildReaderTempDir();
        return await JsonManifestReader.OpenAsync(
            manifestKey,
            tempDir,
            uploader,
            _encryption,
            _loggerFactory.CreateLogger<JsonManifestReader>(),
            ct);
    }

    private async Task<IManifestReader> OpenLegacyAsync(
        string manifestKey, IUploadProvider uploader, CancellationToken ct)
    {
        return await LegacyJsonManifestReader.OpenAsync(manifestKey, uploader, _encryption, ct);
    }

    private string BuildWriterTempDir() =>
        Path.Combine(BuildTempRoot(), "manifest-w-" + Guid.NewGuid().ToString("N"));

    private string BuildReaderTempDir() =>
        Path.Combine(BuildTempRoot(), "manifest-r-" + Guid.NewGuid().ToString("N"));

    private string BuildTempRoot()
    {
        var raw = string.IsNullOrWhiteSpace(_restoreSettings.TempPath) ? "./temp" : _restoreSettings.TempPath;
        return Path.IsPathRooted(raw)
            ? Path.GetFullPath(raw)
            : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, raw));
    }
}
