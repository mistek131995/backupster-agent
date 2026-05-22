using System.Security.Cryptography;
using BackupsterAgent.Services.Backup;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class PgBaseContainerTests
{
    private string _workDir = null!;

    [SetUp]
    public void SetUp()
    {
        _workDir = Path.Combine(Path.GetTempPath(), $"pgbase-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_workDir);
    }

    [TearDown]
    public void TearDown()
    {
        try { if (Directory.Exists(_workDir)) Directory.Delete(_workDir, recursive: true); }
        catch { }
    }

    [Test]
    public async Task WriteThenExtract_RoundTrip_RestoresBytesIdentically()
    {
        var basePayload = RandomNumberGenerator.GetBytes(64 * 1024);
        var walPayload = RandomNumberGenerator.GetBytes(8 * 1024);

        var baseSrc = Path.Combine(_workDir, "src", "base.tar.gz");
        var walSrc = Path.Combine(_workDir, "src", "pg_wal.tar.gz");
        Directory.CreateDirectory(Path.GetDirectoryName(baseSrc)!);
        await File.WriteAllBytesAsync(baseSrc, basePayload);
        await File.WriteAllBytesAsync(walSrc, walPayload);

        var containerPath = Path.Combine(_workDir, "out.pgbase.tar");
        await PgBaseContainer.WriteAsync(containerPath, baseSrc, walSrc, CancellationToken.None);

        Assert.That(File.Exists(containerPath), Is.True, "Container file must be created.");

        var extractDir = Path.Combine(_workDir, "extracted");
        var entries = await PgBaseContainer.ExtractAsync(containerPath, extractDir, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(File.Exists(entries.BaseTarGzPath), Is.True);
            Assert.That(File.Exists(entries.PgWalTarGzPath), Is.True);
            Assert.That(Path.GetFileName(entries.BaseTarGzPath), Is.EqualTo("base.tar.gz"));
            Assert.That(Path.GetFileName(entries.PgWalTarGzPath), Is.EqualTo("pg_wal.tar.gz"));
        });

        var baseRoundTrip = await File.ReadAllBytesAsync(entries.BaseTarGzPath);
        var walRoundTrip = await File.ReadAllBytesAsync(entries.PgWalTarGzPath);

        Assert.That(baseRoundTrip, Is.EqualTo(basePayload));
        Assert.That(walRoundTrip, Is.EqualTo(walPayload));
    }

    [Test]
    public void WriteAsync_MissingBaseFile_Throws()
    {
        var containerPath = Path.Combine(_workDir, "out.pgbase.tar");
        var missingBase = Path.Combine(_workDir, "nope-base.tar.gz");
        var wal = Path.Combine(_workDir, "wal.tar.gz");
        File.WriteAllBytes(wal, new byte[16]);

        Assert.ThrowsAsync<FileNotFoundException>(
            () => PgBaseContainer.WriteAsync(containerPath, missingBase, wal, CancellationToken.None));
    }

    [Test]
    public void WriteAsync_MissingWalFile_Throws()
    {
        var containerPath = Path.Combine(_workDir, "out.pgbase.tar");
        var b = Path.Combine(_workDir, "base.tar.gz");
        File.WriteAllBytes(b, new byte[16]);
        var missingWal = Path.Combine(_workDir, "nope-wal.tar.gz");

        Assert.ThrowsAsync<FileNotFoundException>(
            () => PgBaseContainer.WriteAsync(containerPath, b, missingWal, CancellationToken.None));
    }

    [Test]
    public async Task ExtractAsync_ContainerMissingBaseEntry_Throws()
    {
        var containerPath = Path.Combine(_workDir, "broken.pgbase.tar");
        await using (var fs = File.Create(containerPath))
        await using (var writer = new System.Formats.Tar.TarWriter(fs, System.Formats.Tar.TarEntryFormat.Pax, leaveOpen: false))
        {
            var walTmp = Path.Combine(_workDir, "only-wal.tar.gz");
            await File.WriteAllBytesAsync(walTmp, new byte[16]);
            await writer.WriteEntryAsync(walTmp, "pg_wal.tar.gz", CancellationToken.None);
        }

        var extractDir = Path.Combine(_workDir, "extracted-broken");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PgBaseContainer.ExtractAsync(containerPath, extractDir, CancellationToken.None));
        Assert.That(ex!.Message, Does.Contain("base.tar.gz"));
    }

    [Test]
    public async Task ExtractAsync_ContainerMissingWalEntry_Throws()
    {
        var containerPath = Path.Combine(_workDir, "broken.pgbase.tar");
        await using (var fs = File.Create(containerPath))
        await using (var writer = new System.Formats.Tar.TarWriter(fs, System.Formats.Tar.TarEntryFormat.Pax, leaveOpen: false))
        {
            var baseTmp = Path.Combine(_workDir, "only-base.tar.gz");
            await File.WriteAllBytesAsync(baseTmp, new byte[16]);
            await writer.WriteEntryAsync(baseTmp, "base.tar.gz", CancellationToken.None);
        }

        var extractDir = Path.Combine(_workDir, "extracted-broken");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PgBaseContainer.ExtractAsync(containerPath, extractDir, CancellationToken.None));
        Assert.That(ex!.Message, Does.Contain("pg_wal.tar.gz"));
    }
}
