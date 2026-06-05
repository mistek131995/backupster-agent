using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using BackupsterAgent.Services.Backup;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Restore;
using Microsoft.Extensions.Options;
using Npgsql;

namespace BackupsterAgent.Providers.Restore;

public sealed class PostgresPhysicalRestoreProvider : IRestoreProvider
{
    private const string MarkerFileName = ".backupster-marker";
    private const int OrphanGraceHours = 48;

    private readonly ILogger<PostgresPhysicalRestoreProvider> _logger;
    private readonly PostgresBinaryResolver _binaryResolver;
    private readonly RestoreSettings _restoreSettings;
    private readonly PostgresClusterLifecycle _clusterLifecycle;

    public PostgresPhysicalRestoreProvider(
        ILogger<PostgresPhysicalRestoreProvider> logger,
        PostgresBinaryResolver binaryResolver,
        IOptions<RestoreSettings> restoreSettings,
        PostgresClusterLifecycle clusterLifecycle)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _restoreSettings = restoreSettings.Value;
        _clusterLifecycle = clusterLifecycle;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        var pgCtl = await _binaryResolver.ResolveAsync(connection, "pg_ctl", ct);
        await CheckPgCtlAsync(pgCtl, ct);

        var pgDataPath = await QueryDataDirectoryAsync(connection, ct);
        _logger.LogInformation("Resolved PGDATA from cluster: '{PgDataPath}'", pgDataPath);

        if (!Directory.Exists(pgDataPath))
            throw new RestorePermissionException(
                $"Каталог PGDATA '{pgDataPath}' недоступен на хосте агента. " +
                "Физическое восстановление требует, чтобы агент и PostgreSQL выполнялись на одном хосте.");

        var realPgDataPath = ResolveRealPath(pgDataPath);
        if (!string.Equals(realPgDataPath, pgDataPath, StringComparison.Ordinal))
            _logger.LogInformation(
                "PGDATA '{PgDataPath}' resolves to real path '{RealPath}'. " +
                "Staging/swap operations during restore will use the real parent directory.",
                pgDataPath, realPgDataPath);

        var (parent, _) = SplitPgDataPath(realPgDataPath);
        EnsureSameFsRename(parent, realPgDataPath);

        await _clusterLifecycle.DetectAsync(pgDataPath, ct);
    }

    public Task ValidateRestoreSourceAsync(ConnectionConfig connection, string restoreFilePath, CancellationToken ct) =>
        Task.CompletedTask;

    public Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    public Task RestoreAsync(ConnectionConfig connection, string targetDatabase, string sourceDatabaseName, string restoreFilePath, CancellationToken ct) =>
        ExecuteRestoreAsync(connection, async (stagingPath, populateCt) =>
        {
            _logger.LogInformation(
                "Extracting base archive '{ArchivePath}' to staging '{StagingPath}'", restoreFilePath, stagingPath);
            await ExtractDumpAsync(restoreFilePath, stagingPath, populateCt);
        }, ct);

    internal async Task ExecuteRestoreAsync(
        ConnectionConfig connection,
        Func<string, CancellationToken, Task> populateStagingAsync,
        CancellationToken ct)
    {
        var pgCtl = await _binaryResolver.ResolveAsync(connection, "pg_ctl", ct);
        var pgDataPath = await QueryDataDirectoryAsync(connection, ct);

        if (!Directory.Exists(pgDataPath))
            throw new RestorePermissionException(
                $"Каталог PGDATA '{pgDataPath}' недоступен на хосте агента. " +
                "Физическое восстановление требует, чтобы агент и PostgreSQL выполнялись на одном хосте.");

        var clusterControl = await _clusterLifecycle.DetectAsync(pgDataPath, ct);

        var realPgDataPath = ResolveRealPath(pgDataPath);
        var (parent, leaf) = SplitPgDataPath(realPgDataPath);

        CleanupOrphanStagingDirs(parent, leaf);

        var guid = Guid.NewGuid().ToString("N")[..8];
        var stagingPath = Path.Combine(parent, $"{leaf}.new-{guid}");
        var oldPath = Path.Combine(parent, $"{leaf}.old-{guid}");
        var failedPath = Path.Combine(parent, $"{leaf}.failed-{guid}");
        var startLog = BuildRestoreTempPath("backupster-pg-start") + ".log";

        Directory.CreateDirectory(stagingPath);
        WriteMarkerFile(stagingPath);

        try
        {
            try
            {
                await populateStagingAsync(stagingPath, ct);

                await VerifyStagedClusterAsync(stagingPath, pgCtl, ct);
                _logger.LogInformation("Staged cluster verified at '{StagingPath}'", stagingPath);

                await _clusterLifecycle.PrepareStagingPermissionsAsync(clusterControl, realPgDataPath, stagingPath, ct);

                EnsureSameFsRename(parent, realPgDataPath);
            }
            catch
            {
                TryDeleteDirectory(stagingPath);
                throw;
            }

            _logger.LogInformation("Stopping PostgreSQL cluster at '{PgDataPath}'", pgDataPath);
            try
            {
                await _clusterLifecycle.StopAsync(clusterControl, pgCtl, pgDataPath, ct);
                _logger.LogInformation("PostgreSQL cluster stopped");
            }
            catch
            {
                TryDeleteDirectory(stagingPath);
                throw;
            }

            try
            {
                Directory.Move(realPgDataPath, oldPath);
                Directory.Move(stagingPath, realPgDataPath);

                _logger.LogInformation(
                    "Starting PostgreSQL cluster at '{PgDataPath}' (server log → '{LogFile}')", pgDataPath, startLog);
                await _clusterLifecycle.StartAsync(clusterControl, pgCtl, pgDataPath, startLog, ct);

                _logger.LogInformation("PostgreSQL cluster started");
                TryDeleteDirectory(oldPath);
            }
            catch (Exception swapException)
            {
                await RecoverClusterAsync(
                    clusterControl, pgCtl, pgDataPath, realPgDataPath, stagingPath, oldPath, failedPath, swapException);
                throw new InvalidOperationException(
                    $"Восстановление не удалось ({swapException.Message}). " +
                    "Кластер возвращён в исходное состояние и запущен.",
                    swapException);
            }
        }
        finally
        {
            TryDeleteFile(startLog);
        }
    }

    private async Task RecoverClusterAsync(
        PostgresClusterControl clusterControl, string pgCtl, string pgDataPath, string realPgDataPath,
        string stagingPath, string oldPath, string failedPath,
        Exception originalException)
    {
        _logger.LogError(originalException,
            "Restore swap failed at PGDATA '{PgDataPath}' (real path '{RealPath}'). Attempting recovery.",
            pgDataPath, realPgDataPath);

        await _clusterLifecycle.TryStopForRecoveryAsync(clusterControl, pgCtl, pgDataPath, realPgDataPath);

        var pgdataExists = Directory.Exists(realPgDataPath);
        var oldExists = Directory.Exists(oldPath);

        string? rollbackError = null;

        if (pgdataExists && oldExists)
        {
            _logger.LogWarning(
                "Both PGDATA and backup copy exist. Moving new → '{FailedPath}', restoring backup.", failedPath);

            if (!await TryMoveDirectoryWithRetryAsync(clusterControl, pgCtl, realPgDataPath, failedPath))
                rollbackError =
                    $"новый кластер '{realPgDataPath}' не удалось переместить в '{failedPath}'. " +
                    $"Восстановите вручную: переместите '{realPgDataPath}' в безопасное место, затем '{oldPath}' в '{realPgDataPath}'.";
            else if (!await TryMoveDirectoryWithRetryAsync(clusterControl, pgCtl, oldPath, realPgDataPath))
                rollbackError =
                    $"исходный кластер '{oldPath}' не удалось вернуть в '{realPgDataPath}'. " +
                    $"Восстановите вручную: переместите '{oldPath}' в '{realPgDataPath}'.";
        }
        else if (oldExists && !pgdataExists)
        {
            _logger.LogWarning("PGDATA missing, backup at '{OldPath}'. Restoring.", oldPath);
            if (!await TryMoveDirectoryWithRetryAsync(clusterControl, pgCtl, oldPath, realPgDataPath))
                rollbackError =
                    $"исходный кластер '{oldPath}' не удалось вернуть в '{realPgDataPath}'. " +
                    $"Восстановите вручную: переместите '{oldPath}' в '{realPgDataPath}'.";
        }
        else if (pgdataExists && !oldExists)
        {
            _logger.LogInformation("PGDATA at '{RealPath}' intact, no swap occurred.", realPgDataPath);
        }
        else
        {
            rollbackError =
                $"PGDATA '{realPgDataPath}' и резервная копия '{oldPath}' оба отсутствуют. " +
                "Данные могут быть утеряны. Проверьте файловую систему и логи агента.";
        }

        TryDeleteDirectory(stagingPath);

        if (rollbackError != null)
            throw new InvalidOperationException(
                $"Восстановление не удалось завершить автоматически: {rollbackError} Кластер остановлен.",
                originalException);

        var recoveryLog = BuildRestoreTempPath("backupster-pg-recovery") + ".log";
        try
        {
            _logger.LogInformation("Restarting cluster on original PGDATA at '{PgDataPath}'", pgDataPath);
            await _clusterLifecycle.StartAsync(clusterControl, pgCtl, pgDataPath, recoveryLog, CancellationToken.None);
            _logger.LogInformation("PostgreSQL cluster restarted on original PGDATA after restore failure");
        }
        catch (Exception startException)
        {
            _logger.LogError(startException, "Failed to start cluster after rollback");
            var diagnostics = await _clusterLifecycle.CollectStartDiagnosticsAsync(
                clusterControl, recoveryLog, CancellationToken.None);
            var manualStart = _clusterLifecycle.BuildManualStartInstruction(clusterControl, pgDataPath);
            throw new InvalidOperationException(
                $"Восстановление не удалось завершить автоматически: после отката PGDATA '{pgDataPath}' к исходному состоянию " +
                $"кластер не запускается ({startException.Message}). Запустите вручную: {manualStart}." +
                (string.IsNullOrWhiteSpace(diagnostics) ? "" : $" Диагностика: {diagnostics}"),
                originalException);
        }
        finally
        {
            TryDeleteFile(recoveryLog);
        }
    }

    private async Task<bool> TryMoveDirectoryWithRetryAsync(
        PostgresClusterControl clusterControl, string pgCtl, string from, string to)
    {
        var delays = new[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10) };

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                if (!Directory.Exists(from))
                {
                    _logger.LogWarning("Source directory '{From}' does not exist; cannot move", from);
                    return false;
                }
                Directory.Move(from, to);
                return true;
            }
            catch (Exception ex) when (attempt < delays.Length)
            {
                _logger.LogWarning(ex,
                    "Failed to move '{From}' → '{To}' (attempt {Attempt}/{Total}). Retrying in {Delay}s.",
                    from, to, attempt + 1, delays.Length + 1, delays[attempt].TotalSeconds);

                await _clusterLifecycle.TryStopForRecoveryAsync(clusterControl, pgCtl, from, from);
                await Task.Delay(delays[attempt]);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to move '{From}' → '{To}' after {Total} attempts", from, to, delays.Length + 1);
                return false;
            }
        }
    }

    private async Task CheckPgCtlAsync(string pgCtl, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = pgCtl,
            ArgumentList = { "--version" },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";

        using var process = new Process { StartInfo = psi };
        try
        {
            process.Start();
            await process.WaitForExitAsync(ct);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            throw new RestorePermissionException(
                $"pg_ctl не найден на хосте агента ({ex.Message}). " +
                "Установите postgresql и убедитесь, что pg_ctl есть в PATH.");
        }

        if (process.ExitCode != 0)
            throw new RestorePermissionException(
                $"pg_ctl --version вернул код {process.ExitCode}. " +
                "Убедитесь, что postgresql установлен и pg_ctl есть в PATH.");
    }

    private async Task<string> QueryDataDirectoryAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var result = await PostgresQueryRetry.ExecuteAsync(
            _logger, "SHOW data_directory", connection.Name,
            async innerCt =>
            {
                await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection));
                await conn.OpenAsync(innerCt);

                await using var cmd = new NpgsqlCommand("SHOW data_directory;", conn);
                return await cmd.ExecuteScalarAsync(innerCt);
            }, ct);

        if (result is not string path || string.IsNullOrWhiteSpace(path))
            throw new RestorePermissionException(
                $"Не удалось получить путь PGDATA из кластера '{connection.Name}'.");

        return path;
    }

    internal async Task ExtractDumpAsync(string dumpPath, string targetDir, CancellationToken ct)
    {
        var format = await PgBaseFormatDetector.DetectByContentAsync(dumpPath, ct);

        if (format == PgBaseDumpFormat.LegacySingleTarGz)
        {
            _logger.LogInformation(
                "Detected legacy single-tar PostgreSQL dump (gzip magic) at '{Path}'. Extracting directly into '{TargetDir}'.",
                dumpPath, targetDir);
            await ExtractTarGzAsync(dumpPath, targetDir, ct);
            return;
        }

        _logger.LogInformation(
            "Detected pgbase container (ustar magic) at '{Path}'. Unpacking container, then extracting base and pg_wal into '{TargetDir}'.",
            dumpPath, targetDir);

        var workDir = BuildRestoreTempPath("backupster-pgbase");
        Directory.CreateDirectory(workDir);

        try
        {
            var entries = await PgBaseContainer.ExtractAsync(dumpPath, workDir, ct);

            await ExtractTarGzAsync(entries.BaseTarGzPath, targetDir, ct);

            var pgWalDir = Path.Combine(targetDir, "pg_wal");
            Directory.CreateDirectory(pgWalDir);
            await ExtractTarGzAsync(entries.PgWalTarGzPath, pgWalDir, ct);
        }
        finally
        {
            TryDeleteDirectory(workDir);
        }
    }

    internal async Task ExtractTarGzAsync(string archivePath, string targetDir, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "tar",
            ArgumentList = { "-xzf", archivePath, "-C", targetDir },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        using var process = new Process { StartInfo = psi };
        process.Start();
        _logger.LogInformation("tar process started (PID {Pid})", process.Id);

        try
        {
            var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
            var stderrTask = process.StandardError.ReadToEndAsync(ct);

            await Task.WhenAll(stdoutTask, stderrTask);
            await process.WaitForExitAsync(ct);

            if (process.ExitCode != 0)
            {
                var stderr = stderrTask.Result.Trim();
                var stdout = stdoutTask.Result.Trim();
                var detail = string.IsNullOrEmpty(stderr) ? stdout : stderr;
                throw new InvalidOperationException($"Распаковка tar завершилась с ошибкой (код {process.ExitCode}): {detail}");
            }
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to kill tar process"); }
            }
            throw;
        }
    }

    private static (string parent, string leaf) SplitPgDataPath(string path)
    {
        var trimmed = path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var parent = Path.GetDirectoryName(trimmed);
        var leaf = Path.GetFileName(trimmed);
        if (string.IsNullOrEmpty(parent) || string.IsNullOrEmpty(leaf))
            throw new InvalidOperationException(
                $"Не удалось разобрать путь PGDATA '{path}' на родительский каталог и имя.");
        return (parent, leaf);
    }

    private string ResolveRealPath(string pgDataPath)
    {
        // We resolve only the PGDATA leaf itself. If a parent directory is a symlink, the OS follows it
        // transparently during rename(2)/MoveFile, so no separate handling is needed for parent links.
        var fullPath = Path.GetFullPath(pgDataPath);
        try
        {
            var info = new DirectoryInfo(fullPath);
            var target = info.ResolveLinkTarget(returnFinalTarget: true);
            return target?.FullName is { Length: > 0 } realPath ? realPath : fullPath;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to resolve symlinks for PGDATA '{PgDataPath}'. Falling back to original path; " +
                "if PGDATA is a symlink to another mount, restore may place data on the wrong volume — " +
                "explicitly point PGDATA at the real path or fix permissions/link integrity to silence this warning.",
                fullPath);
            return fullPath;
        }
    }

    private void EnsureSameFsRename(string parent, string realPgDataPath)
    {
        var probeFromParent = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeToParent = Path.Combine(parent, $"backupster-rename-probe-{Guid.NewGuid():N}");
        var probeInside = Path.Combine(realPgDataPath, $"backupster-rename-probe-{Guid.NewGuid():N}");
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
            throw new InvalidOperationException(
                $"Не удалось выполнить атомарный rename для PGDATA '{realPgDataPath}'. " +
                $"Physical restore требует, чтобы PGDATA и её родительский каталог '{parent}' поддерживали атомарный rename внутри одной FS. " +
                "Не подходят: PGDATA — отдельная точка монтирования Linux (например, '/mnt/db' смонтирован как сама PGDATA); " +
                "Windows volume mount point; cross-FS симлинк, который не удалось разрешить (см. предыдущий warning о ResolveLinkTarget). " +
                $"Подробности: {ex.Message}", ex);
        }
        finally
        {
            TryDeleteDirectory(probeToParent);
            TryDeleteDirectory(probeOutside);
        }
    }

    internal static void WriteMarkerFile(string dir)
    {
        var path = Path.Combine(dir, MarkerFileName);
        File.WriteAllText(path, DateTime.UtcNow.ToString("o"));
    }

    private void CleanupOrphanStagingDirs(string parent, string leaf)
    {
        string[] suffixes = ["new", "failed"];
        var threshold = DateTime.UtcNow - TimeSpan.FromHours(OrphanGraceHours);

        foreach (var suffix in suffixes)
        {
            IEnumerable<string> matches;
            try
            {
                matches = Directory.EnumerateDirectories(parent, $"{leaf}.{suffix}-*");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Orphan cleanup: failed to enumerate '{Parent}' for pattern '{Leaf}.{Suffix}-*'",
                    parent, leaf, suffix);
                continue;
            }

            foreach (var dir in matches)
            {
                try
                {
                    var marker = Path.Combine(dir, MarkerFileName);
                    if (!File.Exists(marker))
                    {
                        _logger.LogDebug(
                            "Orphan cleanup: '{Dir}' has no '{Marker}' marker, leaving alone",
                            dir, MarkerFileName);
                        continue;
                    }

                    DateTime createdAt;
                    try
                    {
                        var content = File.ReadAllText(marker).Trim();
                        if (!DateTime.TryParse(content, null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out createdAt))
                        {
                            _logger.LogDebug(
                                "Orphan cleanup: '{Dir}' marker has unparseable timestamp '{Content}', leaving alone",
                                dir, content);
                            continue;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex,
                            "Orphan cleanup: '{Dir}' marker unreadable, leaving alone", dir);
                        continue;
                    }

                    if (createdAt > threshold)
                    {
                        _logger.LogDebug(
                            "Orphan cleanup: '{Dir}' marker created {CreatedAt:o}, younger than {Hours}h, leaving alone",
                            dir, createdAt, OrphanGraceHours);
                        continue;
                    }

                    _logger.LogWarning(
                        "Orphan cleanup: deleting stale staging dir '{Dir}' (marker created {CreatedAt:o}, age > {Hours}h)",
                        dir, createdAt, OrphanGraceHours);
                    Directory.Delete(dir, recursive: true);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Orphan cleanup: failed to process '{Dir}'", dir);
                }
            }
        }
    }

    private async Task VerifyStagedClusterAsync(string stagingPath, string pgCtl, CancellationToken ct)
    {
        var versionFile = Path.Combine(stagingPath, "PG_VERSION");
        if (!File.Exists(versionFile))
            throw new InvalidOperationException(
                $"Распакованный архив в '{stagingPath}' не содержит PG_VERSION — " +
                "это не похоже на PGDATA от pg_basebackup. Возможно, архив повреждён.");

        if (!Directory.Exists(Path.Combine(stagingPath, "global")))
            throw new InvalidOperationException(
                $"Распакованный архив в '{stagingPath}' не содержит каталога 'global'. " +
                "Возможно, архив повреждён.");

        var pgTblspc = Path.Combine(stagingPath, "pg_tblspc");
        if (Directory.Exists(pgTblspc))
        {
            var entries = Directory.GetFileSystemEntries(pgTblspc);
            if (entries.Length > 0)
            {
                var oids = string.Join(", ", entries.Select(Path.GetFileName));
                throw new InvalidOperationException(
                    $"Бэкап содержит tablespaces (pg_tblspc/{{{oids}}}), но physical-режим Backupster их не поддерживает: " +
                    "данные tablespace не шиппятся в архиве, после restore остались бы битые симлинки. " +
                    "Бэкап создан старой версией агента до guard'а — пересоздайте его в logical-режиме либо удалите tablespaces в источнике.");
            }
        }

        var archiveMajor = (await File.ReadAllTextAsync(versionFile, ct)).Trim();
        var pgCtlMajor = await GetPgCtlMajorVersionAsync(pgCtl, ct);

        if (!string.Equals(archiveMajor, pgCtlMajor, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Мажорная версия бэкапа (PG_VERSION={archiveMajor}) не совпадает с версией pg_ctl ({pgCtlMajor}). " +
                "Восстановите бэкап на PostgreSQL той же мажорной версии.");
    }

    private async Task<string> GetPgCtlMajorVersionAsync(string pgCtl, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = pgCtl,
            ArgumentList = { "--version" },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        psi.Environment["LC_MESSAGES"] = "C";
        psi.Environment["LANG"] = "C";

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stdout = await process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);

        var match = Regex.Match(stdout, @"\(PostgreSQL\)\s+(\d+)");
        if (!match.Success)
            throw new InvalidOperationException(
                $"Не удалось определить мажорную версию pg_ctl. Вывод '--version': '{stdout.Trim()}'.");
        return match.Groups[1].Value;
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
            _logger.LogWarning(ex, "Failed to delete directory '{Path}'", path);
        }
    }

    private void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete file '{Path}'", path);
        }
    }

    private string BuildRestoreTempPath(string prefix)
    {
        var root = DatabaseRestoreService.BuildTempRoot(_restoreSettings.TempPath);
        Directory.CreateDirectory(root);
        return Path.Combine(root, $"{prefix}-{Guid.NewGuid():N}");
    }

}
