using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Providers.Restore.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlDatadirSwapper : IMysqlDatadirSwapper
{
    private const int OrphanGraceHours = 48;

    private readonly ILogger<MysqlDatadirSwapper> _logger;
    private readonly RestoreSettings _restoreSettings;
    private readonly RestorePathResolver _pathResolver;
    private readonly RestoreMarkerStore _markerStore;
    private readonly FilesystemRenamePreflight _renamePreflight;

    public MysqlDatadirSwapper(
        ILogger<MysqlDatadirSwapper> logger,
        IOptions<RestoreSettings> restoreSettings,
        RestorePathResolver pathResolver,
        RestoreMarkerStore markerStore,
        FilesystemRenamePreflight renamePreflight)
    {
        _logger = logger;
        _restoreSettings = restoreSettings.Value;
        _pathResolver = pathResolver;
        _markerStore = markerStore;
        _renamePreflight = renamePreflight;
    }

    public MysqlDatadirSwapper(
        ILogger<MysqlDatadirSwapper> logger,
        IOptions<RestoreSettings> restoreSettings)
        : this(
            logger,
            restoreSettings,
            new RestorePathResolver(NullLogger<RestorePathResolver>.Instance),
            new RestoreMarkerStore(NullLogger<RestoreMarkerStore>.Instance),
            new FilesystemRenamePreflight(NullLogger<FilesystemRenamePreflight>.Instance))
    {
    }

    public string ResolveRealPath(string path) =>
        _pathResolver.ResolveRealPath(path, "MySQL datadir");

    public void EnsureSameFsRename(string parent, string realPath) =>
        _renamePreflight.EnsureSameFsRename(parent, realPath, "MySQL datadir", throwRestorePermissionException: true);

    public static (string parent, string leaf) SplitPath(string path) =>
        RestorePathResolver.SplitPath(path, "MySQL datadir");

    public static void WriteMarkerFile(string dir) =>
        RestoreMarkerStore.WriteMarkerFile(dir);

    public void CleanupOrphanStagingDirs(string parent, string leaf) =>
        _markerStore.CleanupOrphanStagingDirs(
            parent, leaf, ["new", "failed", "old"], TimeSpan.FromHours(OrphanGraceHours));

    public async Task FixOwnershipAsync(string newDatadir, MysqlInstanceInfo instanceInfo, CancellationToken ct)
    {
        if (instanceInfo is not { OwnerUser: not null, OwnerGroup: not null })
            throw new InvalidOperationException(
                "Не удалось определить владельца каталога данных MySQL. " +
                "Невозможно восстановить права доступа после подмены datadir.");

        var ownerSpec = $"{instanceInfo.OwnerUser}:{instanceInfo.OwnerGroup}";

        var timeoutSeconds = Math.Max(_restoreSettings.ChownTimeoutSeconds, 1);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        var psi = new ProcessStartInfo
        {
            FileName = "chown",
            ArgumentList = { "-R", ownerSpec, newDatadir },
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        using var process = new Process { StartInfo = psi };
        process.Start();

        var stderrTask = process.StandardError.ReadToEndAsync(timeoutCts.Token);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(timeoutCts.Token);

        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch { }

            if (ct.IsCancellationRequested)
            {
                _logger.LogWarning(
                    "chown -R {OwnerSpec} '{Path}' aborted by user cancellation - process killed",
                    ownerSpec, newDatadir);
                throw;
            }

            _logger.LogError(
                "chown -R {OwnerSpec} '{Path}' timed out after {Timeout}s - process killed",
                ownerSpec, newDatadir, timeoutSeconds);
            throw new InvalidOperationException(
                $"Смена владельца каталога данных MySQL не завершилась за {timeoutSeconds} секунд. " +
                "Восстановление прервано.");
        }

        var stderr = "";
        try { stderr = await stderrTask; } catch { }
        try { await stdoutTask; } catch { }

        if (process.ExitCode != 0)
        {
            _logger.LogError(
                "chown -R {OwnerSpec} '{Path}' exited with code {ExitCode}: {Stderr}",
                ownerSpec, newDatadir, process.ExitCode, stderr);
            throw new InvalidOperationException(
                $"Не удалось сменить владельца каталога данных MySQL (код выхода {process.ExitCode}).");
        }

        _logger.LogInformation("Fixed ownership to {OwnerSpec} on '{Path}'", ownerSpec, newDatadir);
    }

    public void MoveDirectory(string from, string to) => Directory.Move(from, to);

    public bool DirectoryExists(string path) => Directory.Exists(path);

    public bool TryMoveDirectory(string from, string to)
    {
        try
        {
            if (!Directory.Exists(from))
                return false;

            Directory.Move(from, to);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move '{From}' to '{To}'", from, to);
            return false;
        }
    }

    public void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete directory '{Path}'", path);
        }
    }
}
