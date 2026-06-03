using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Upload;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Restore;

namespace BackupsterAgent.IntegrationTests.Backup;

internal static class FileManifestV2IntegrationAssert
{
    public static async Task RunAsync(
        ManifestStore manifestStore,
        FileBackupService fileBackup,
        FileRestoreService fileRestore,
        IUploadProvider provider,
        string tempRoot,
        CancellationToken ct)
    {
        var rootA = Path.Combine(tempRoot, "v2-root-a");
        var rootB = Path.Combine(tempRoot, "v2-root-b");
        var relPath = Path.Combine("shared", "file.txt");
        var relManifestPath = relPath.Replace('\\', '/');

        Directory.CreateDirectory(Path.Combine(rootA, "shared"));
        Directory.CreateDirectory(Path.Combine(rootB, "shared"));

        var contentA = new byte[] { 1, 2, 3, 4 };
        var contentB = new byte[] { 9, 8, 7, 6 };
        await File.WriteAllBytesAsync(Path.Combine(rootA, relPath), contentA, ct);
        await File.WriteAllBytesAsync(Path.Combine(rootB, relPath), contentB, ct);

        var roots = new[] { rootA, rootB };
        var reporter = new NullProgressReporter<BackupStage>();

        string manifestKey;
        await using (var writer = manifestStore.OpenWriter("itest-db", dumpObjectKey: string.Empty, roots))
        {
            await fileBackup.CaptureAsync(roots, provider, writer, reporter, ct);
            manifestKey = await writer.CompleteAsync(provider, "manifest-v2", ct);
        }

        await using (var reader = await manifestStore.OpenReaderAsync(manifestKey, provider, ct))
        {
            Assert.That(reader.Meta.SchemaVersion, Is.EqualTo(2));
            Assert.That(reader.Meta.Roots, Is.EqualTo(roots));

            var entries = new List<BackupsterAgent.Domain.FileEntry>();
            await foreach (var entry in reader.ReadFilesAsync(ct))
                entries.Add(entry);

            Assert.Multiple(() =>
            {
                Assert.That(entries, Has.Count.EqualTo(2));
                Assert.That(entries.Select(e => e.Path), Is.All.EqualTo(relManifestPath));
                Assert.That(entries.Select(e => e.RootIndex), Is.EquivalentTo(new[] { 0, 1 }));
            });
        }

        Directory.Delete(rootA, recursive: true);
        Directory.Delete(rootB, recursive: true);

        var restoreResult = await fileRestore.RunAsync(
            manifestKey,
            targetFileRoot: null,
            provider,
            new NullProgressReporter<RestoreStage>(),
            ct);

        Assert.That(restoreResult.Status, Is.EqualTo(RestoreFilesStatus.Success), restoreResult.ErrorMessage);
        Assert.That(restoreResult.FilesRestoredCount, Is.EqualTo(2));
        Assert.That(await File.ReadAllBytesAsync(Path.Combine(rootA, relPath), ct), Is.EqualTo(contentA));
        Assert.That(await File.ReadAllBytesAsync(Path.Combine(rootB, relPath), ct), Is.EqualTo(contentB));
    }
}
