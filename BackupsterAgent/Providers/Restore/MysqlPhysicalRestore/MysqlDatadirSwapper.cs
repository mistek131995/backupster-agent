using System.Diagnostics;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlDatadirSwapper : IMysqlDatadirSwapper
{
    private const string MarkerFileName = ".backupster-marker";
    private const int OrphanGraceHours = 48;

    private readonly ILogger<MysqlDatadirSwapper> _logger;
    private readonly RestoreSettings _restoreSettings;

    public MysqlDatadirSwapper(
        ILogger<MysqlDatadirSwapper> logger,
        IOptions<RestoreSettings> restoreSettings)
    {
        _logger = logger;
        _restoreSettings = restoreSettings.Value;
    }

    public string ResolveRealPath(string path)
    {
        var fullPath = Path.GetFullPath(path);
        try
        {
            var info = new DirectoryInfo(fullPath);
            var target = info.ResolveLinkTarget(returnFinalTarget: true);
            return target?.FullName is { Length: > 0 } realPath ? realPath : fullPath;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to resolve symlinks for MySQL datadir '{Path}'. Using original path.", fullPath);
            return fullPath;
        }
    }

    public void EnsureSameFsRename(string parent, string realPath)
    {
        var probeFrom = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeTo = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(probeFrom);
            Directory.Move(probeFrom, probeTo);
        }
        catch (Exception ex)
        {
            TryDeleteDirectory(probeFrom);
            TryDeleteDirectory(probeTo);
            throw new RestorePermissionException(
                $"Не удалось выполнить атомарный rename для MySQL datadir '{realPath}'. " +
                $"Физическое восстановление требует, чтобы datadir и его родительский каталог '{parent}' " +
                "поддерживали атомарный rename внутри одной файловой системы. " +
                $"Подробности: {ex.Message}");
        }
        finally
        {
            TryDeleteDirectory(probeTo);
        }
    }

    public static (string parent, string leaf) SplitPath(string path)
    {
        var trimmed = path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var parent = Path.GetDirectoryName(trimmed);
        var leaf = Path.GetFileName(trimmed);
        if (string.IsNullOrEmpty(parent) || string.IsNullOrEmpty(leaf))
            throw new InvalidOperationException(
                $"Не удалось разобрать путь '{path}' на родительский каталог и имя.");
        return (parent, leaf);
    }

    public static void WriteMarkerFile(string dir)
    {
        var path = Path.Combine(dir, MarkerFileName);
        File.WriteAllText(path, DateTime.UtcNow.ToString("o"));
    }

    public void CleanupOrphanStagingDirs(string parent, string leaf)
    {
        string[] suffixes = ["new", "failed", "old"];
        var threshold = DateTime.UtcNow - TimeSpan.FromHours(OrphanGraceHours);

        foreach (var suffix in suffixes)
        {
            IEnumerable<string> matches;
            try
            {
                matches = Directory.EnumerateDirectories(parent, $"{leaf}.{suffix}-*");
            }
            catch
            {
                continue;
            }

            foreach (var dir in matches)
            {
                try
                {
                    var marker = Path.Combine(dir, MarkerFileName);
                    if (!File.Exists(marker)) continue;

                    var content = File.ReadAllText(marker).Trim();
                    if (!DateTime.TryParse(content, null,
                            System.Globalization.DateTimeStyles.AssumeUniversal |
                            System.Globalization.DateTimeStyles.AdjustToUniversal,
                            out var createdAt))
                        continue;

                    if (createdAt > threshold) continue;

                    _logger.LogWarning(
                        "Orphan cleanup: deleting stale staging dir '{Dir}' (age > {Hours}h)",
                        dir, OrphanGraceHours);
                    Directory.Delete(dir, recursive: true);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Orphan cleanup: failed to process '{Dir}'", dir);
                }
            }
        }
    }

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
                    "chown -R {OwnerSpec} '{Path}' aborted by user cancellation — process killed",
                    ownerSpec, newDatadir);
                throw;
            }

            _logger.LogError(
                "chown -R {OwnerSpec} '{Path}' timed out after {Timeout}s — process killed",
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
