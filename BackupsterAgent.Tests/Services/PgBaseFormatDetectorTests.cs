using System.IO.Compression;
using System.Security.Cryptography;
using BackupsterAgent.Services.Backup;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class PgBaseFormatDetectorTests
{
    private string _workDir = null!;

    [SetUp]
    public void SetUp()
    {
        _workDir = Path.Combine(Path.GetTempPath(), $"pgbase-detect-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_workDir);
    }

    [TearDown]
    public void TearDown()
    {
        try { if (Directory.Exists(_workDir)) Directory.Delete(_workDir, recursive: true); }
        catch { }
    }

    [TestCase("mydb_20260419_030000.pgbase.tar", PgBaseDumpFormat.Container)]
    [TestCase("mydb_20260419_030000.pgbase.tar.enc", PgBaseDumpFormat.Container)]
    [TestCase("mydb_20260419_030000_diff.pgbase.tar", PgBaseDumpFormat.Container)]
    [TestCase("mydb_20260419_030000_diff.pgbase.tar.enc", PgBaseDumpFormat.Container)]
    [TestCase("payments/2026-04-19_03-00-00/mydb_20260419_030000.pgbase.tar.enc", PgBaseDumpFormat.Container)]
    [TestCase("MYDB_20260419_030000.PGBASE.TAR.ENC", PgBaseDumpFormat.Container)]
    [TestCase("mydb_20260419_030000.tar.gz", PgBaseDumpFormat.LegacySingleTarGz)]
    [TestCase("mydb_20260419_030000.tar.gz.enc", PgBaseDumpFormat.LegacySingleTarGz)]
    [TestCase("mydb_20260419_030000_diff.tar.gz.enc", PgBaseDumpFormat.LegacySingleTarGz)]
    [TestCase("payments/2026-04-19_03-00-00/mydb_20260419_030000.tar.gz.enc", PgBaseDumpFormat.LegacySingleTarGz)]
    [TestCase("", PgBaseDumpFormat.LegacySingleTarGz)]
    [TestCase("   ", PgBaseDumpFormat.LegacySingleTarGz)]
    public void DetectByName_ReturnsExpectedFormat(string input, PgBaseDumpFormat expected)
    {
        var actual = PgBaseFormatDetector.DetectByName(input);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task DetectByContentAsync_GzipFile_ReturnsLegacy()
    {
        var path = Path.Combine(_workDir, "real.gz");
        await using (var fs = File.Create(path))
        await using (var gz = new GZipStream(fs, CompressionLevel.Fastest, leaveOpen: false))
        {
            var payload = RandomNumberGenerator.GetBytes(4096);
            await gz.WriteAsync(payload);
        }

        var format = await PgBaseFormatDetector.DetectByContentAsync(path, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.LegacySingleTarGz));
    }

    [Test]
    public async Task DetectByContentAsync_PgBaseContainer_ReturnsContainer()
    {
        var basePath = Path.Combine(_workDir, "base.tar.gz");
        var walPath = Path.Combine(_workDir, "pg_wal.tar.gz");
        await File.WriteAllBytesAsync(basePath, RandomNumberGenerator.GetBytes(256));
        await File.WriteAllBytesAsync(walPath, RandomNumberGenerator.GetBytes(128));

        var containerPath = Path.Combine(_workDir, "container.pgbase.tar");
        await PgBaseContainer.WriteAsync(containerPath, basePath, walPath, CancellationToken.None);

        var format = await PgBaseFormatDetector.DetectByContentAsync(containerPath, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.Container));
    }

    [Test]
    public async Task DetectByContentAsync_PgBaseContainerRenamedToDumpBin_ReturnsContainer()
    {
        var basePath = Path.Combine(_workDir, "base.tar.gz");
        var walPath = Path.Combine(_workDir, "pg_wal.tar.gz");
        await File.WriteAllBytesAsync(basePath, RandomNumberGenerator.GetBytes(256));
        await File.WriteAllBytesAsync(walPath, RandomNumberGenerator.GetBytes(128));

        var containerPath = Path.Combine(_workDir, "tmp.pgbase.tar");
        await PgBaseContainer.WriteAsync(containerPath, basePath, walPath, CancellationToken.None);

        var dumpBinPath = Path.Combine(_workDir, "dump.bin");
        File.Move(containerPath, dumpBinPath);

        var format = await PgBaseFormatDetector.DetectByContentAsync(dumpBinPath, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.Container),
            "Sniff must recognise pgbase container by ustar magic regardless of file name.");
    }

    [Test]
    public async Task DetectByContentAsync_GzipRenamedToDumpBin_ReturnsLegacy()
    {
        var dumpBinPath = Path.Combine(_workDir, "dump.bin");
        await using (var fs = File.Create(dumpBinPath))
        await using (var gz = new GZipStream(fs, CompressionLevel.Fastest, leaveOpen: false))
        {
            await gz.WriteAsync(RandomNumberGenerator.GetBytes(2048));
        }

        var format = await PgBaseFormatDetector.DetectByContentAsync(dumpBinPath, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.LegacySingleTarGz),
            "Sniff must recognise legacy gzipped tar by gzip magic regardless of file name.");
    }

    [Test]
    public async Task DetectByContentAsync_EmptyFile_ReturnsLegacyAsSafeDefault()
    {
        var path = Path.Combine(_workDir, "empty.bin");
        await File.WriteAllBytesAsync(path, []);

        var format = await PgBaseFormatDetector.DetectByContentAsync(path, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.LegacySingleTarGz));
    }

    [Test]
    public async Task DetectByContentAsync_ShortRandomBytes_ReturnsLegacyAsSafeDefault()
    {
        var path = Path.Combine(_workDir, "short.bin");
        await File.WriteAllBytesAsync(path, [0x00, 0x01, 0x02, 0x03]);

        var format = await PgBaseFormatDetector.DetectByContentAsync(path, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.LegacySingleTarGz));
    }

    [Test]
    public async Task DetectByContentAsync_RandomLargeBlob_ReturnsLegacyAsSafeDefault()
    {
        var path = Path.Combine(_workDir, "random.bin");
        await File.WriteAllBytesAsync(path, RandomNumberGenerator.GetBytes(8192));

        var format = await PgBaseFormatDetector.DetectByContentAsync(path, CancellationToken.None);
        Assert.That(format, Is.EqualTo(PgBaseDumpFormat.LegacySingleTarGz));
    }
}
