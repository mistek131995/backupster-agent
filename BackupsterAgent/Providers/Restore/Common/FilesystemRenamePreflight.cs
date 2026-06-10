using BackupsterAgent.Exceptions;

namespace BackupsterAgent.Providers.Restore.Common;

public sealed class FilesystemRenamePreflight
{
    private readonly ILogger<FilesystemRenamePreflight> _logger;

    public FilesystemRenamePreflight(ILogger<FilesystemRenamePreflight> logger)
    {
        _logger = logger;
    }

    public void EnsureSameFsRename(string parent, string livePath, string subject, bool throwRestorePermissionException)
    {
        var probeFromParent = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeToParent = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeInside = Path.Combine(livePath, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeOutside = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(probeFromParent);
            Directory.Move(probeFromParent, probeToParent);

            Directory.CreateDirectory(probeInside);
            Directory.Move(probeInside, probeOutside);
        }
        catch (Exception ex)
        {
            TryDeleteDirectory(probeFromParent);
            TryDeleteDirectory(probeToParent);
            TryDeleteDirectory(probeInside);
            TryDeleteDirectory(probeOutside);

            var message =
                $"Не удалось выполнить атомарный rename для {subject} '{livePath}'. " +
                $"Physical restore требует, чтобы каталог и его родительский каталог '{parent}' поддерживали атомарный rename внутри одной файловой системы. " +
                "Не подходят: отдельная точка монтирования Linux прямо на data directory, Windows volume mount point, cross-FS symlink или каталог без прав на rename.";

            if (throwRestorePermissionException)
                throw new RestorePermissionException(message, ex);

            throw new InvalidOperationException(message, ex);
        }
        finally
        {
            TryDeleteDirectory(probeToParent);
            TryDeleteDirectory(probeOutside);
        }
    }

    private void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to delete rename preflight probe '{Path}'", path);
        }
    }
}
