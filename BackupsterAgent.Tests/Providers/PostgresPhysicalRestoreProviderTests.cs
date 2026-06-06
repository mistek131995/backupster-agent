using System.Formats.Tar;
using System.IO.Compression;
using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Providers;

[TestFixture]
public sealed class PostgresPhysicalRestoreProviderTests
{
    private string _workDir = null!;

    [SetUp]
    public void SetUp()
    {
        _workDir = Path.Combine(Path.GetTempPath(), $"pg-restore-source-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_workDir);
    }

    [TearDown]
    public void TearDown()
    {
        try { if (Directory.Exists(_workDir)) Directory.Delete(_workDir, recursive: true); }
        catch { }
    }

    [Test]
    public async Task ValidateRestoreSourceAsync_PgBaseContainerWithBaseAndWal_Succeeds()
    {
        var provider = CreateProvider();
        var path = await CreateContainerAsync(includeBase: true, includeWal: true);

        Assert.DoesNotThrowAsync(() =>
            provider.ValidateRestoreSourceAsync(Connection(), path, CancellationToken.None));
    }

    [Test]
    public async Task ValidateRestoreSourceAsync_PgBaseContainerMissingWal_Throws()
    {
        var provider = CreateProvider();
        var path = await CreateContainerAsync(includeBase: true, includeWal: false);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.ValidateRestoreSourceAsync(Connection(), path, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain(PgBaseContainer.WalEntryName));
    }

    [Test]
    public async Task ValidateRestoreSourceAsync_LegacyGzipArchive_Succeeds()
    {
        var provider = CreateProvider();
        var path = Path.Combine(_workDir, "legacy.tar.gz");
        await WriteGzipAsync(path);

        Assert.DoesNotThrowAsync(() =>
            provider.ValidateRestoreSourceAsync(Connection(), path, CancellationToken.None));
    }

    [Test]
    public async Task ValidateRestoreSourceAsync_RandomBytes_Throws()
    {
        var provider = CreateProvider();
        var path = Path.Combine(_workDir, "dump.bin");
        await File.WriteAllBytesAsync(path, [0x00, 0x01, 0x02, 0x03, 0x04]);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.ValidateRestoreSourceAsync(Connection(), path, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("pgbase").And.Contain("legacy gzip"));
    }

    [Test]
    public async Task ValidateRestoreSourceAsync_EmptyFile_Throws()
    {
        var provider = CreateProvider();
        var path = Path.Combine(_workDir, "empty.bin");
        await File.WriteAllBytesAsync(path, []);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.ValidateRestoreSourceAsync(Connection(), path, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("пустой"));
    }

    [Test]
    public void CheckTarAsync_MissingBinary_ThrowsPermissionHint()
    {
        var missingTar = Path.Combine(_workDir, "missing-tar-binary");

        var ex = Assert.ThrowsAsync<RestorePermissionException>(() =>
            PostgresPhysicalRestoreProvider.CheckTarAsync(missingTar, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("tar").And.Contain("PATH"));
    }

    private async Task<string> CreateContainerAsync(bool includeBase, bool includeWal)
    {
        var sourceDir = Path.Combine(_workDir, "src-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(sourceDir);

        var containerPath = Path.Combine(_workDir, "backup.pgbase.tar");

        await using var fs = File.Create(containerPath);
        await using var writer = new TarWriter(fs, TarEntryFormat.Pax, leaveOpen: false);

        if (includeBase)
        {
            var basePath = Path.Combine(sourceDir, PgBaseContainer.BaseEntryName);
            await WriteGzipAsync(basePath);
            await writer.WriteEntryAsync(basePath, PgBaseContainer.BaseEntryName, CancellationToken.None);
        }

        if (includeWal)
        {
            var walPath = Path.Combine(sourceDir, PgBaseContainer.WalEntryName);
            await WriteGzipAsync(walPath);
            await writer.WriteEntryAsync(walPath, PgBaseContainer.WalEntryName, CancellationToken.None);
        }

        return containerPath;
    }

    private static async Task WriteGzipAsync(string path)
    {
        await using var fs = File.Create(path);
        await using var gz = new GZipStream(fs, CompressionLevel.Fastest);
        await gz.WriteAsync(new byte[] { 0x42 });
    }

    private static PostgresPhysicalRestoreProvider CreateProvider()
    {
        var settings = Options.Create(new RestoreSettings());
        var runner = new StubProcessRunner();
        var lifecycle = new PostgresClusterLifecycle(
            NullLogger<PostgresClusterLifecycle>.Instance,
            runner,
            settings);

        return new PostgresPhysicalRestoreProvider(
            NullLogger<PostgresPhysicalRestoreProvider>.Instance,
            new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance),
            settings,
            lifecycle);
    }

    private static ConnectionConfig Connection() => new()
    {
        Name = "pg-main",
        DatabaseType = DatabaseType.Postgres,
        Host = "localhost",
        Port = 5432,
        Username = "postgres",
        Password = string.Empty,
    };

    private sealed class StubProcessRunner : IExternalProcessRunner
    {
        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct) =>
            Task.FromResult(new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            });
    }
}
