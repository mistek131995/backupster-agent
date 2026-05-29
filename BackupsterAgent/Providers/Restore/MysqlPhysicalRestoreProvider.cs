using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using BackupsterAgent.Services.Restore;
using Microsoft.Extensions.Options;
using System.Net.Sockets;
using MySqlConnector;

namespace BackupsterAgent.Providers.Restore;

public sealed class MysqlPhysicalRestoreProvider : IRestoreProvider
{
    private const string MarkerFileName = ".backupster-marker";
    private const int OrphanGraceHours = 48;

    private record MysqlInstanceInfo(IReadOnlyList<string> OriginalArgs, int? Pid, string? OwnerUser, string? OwnerGroup, string? ServiceName);

    internal static readonly IReadOnlySet<string> SensitiveMysqldKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "--password",
        "--admin-password",
        "--init-file",
        "--init-connect",
        "--ssl-key",
        "--ssl-cert",
        "--ssl-ca",
        "--tls-key",
        "--tls-cert",
        "--plugin-load",
        "--plugin-load-add",
        "--early-plugin-load",
        "--skip-grant-tables",
    };

    private readonly ILogger<MysqlPhysicalRestoreProvider> _logger;
    private readonly MysqlBinaryResolver _binaryResolver;
    private readonly IExternalProcessRunner _processRunner;
    private readonly RestoreSettings _restoreSettings;

    public MysqlPhysicalRestoreProvider(
        ILogger<MysqlPhysicalRestoreProvider> logger,
        MysqlBinaryResolver binaryResolver,
        IExternalProcessRunner processRunner,
        IOptions<RestoreSettings> restoreSettings)
    {
        _logger = logger;
        _binaryResolver = binaryResolver;
        _processRunner = processRunner;
        _restoreSettings = restoreSettings.Value;
    }

    public async Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct)
    {
        EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        await EnsureBinaryAvailableAsync(xtrabackup, "xtrabackup", ct);

        var xbstream = _binaryResolver.Resolve(connection, "xbstream");
        await EnsureBinaryAvailableAsync(xbstream, "xbstream", ct);

        var datadir = await QueryDataDirectoryAsync(connection, ct);
        _logger.LogInformation("Resolved MySQL datadir: '{DataDir}'", datadir);

        if (!Directory.Exists(datadir))
            throw new RestorePermissionException(
                $"Каталог данных MySQL '{datadir}' недоступен на хосте агента. " +
                "Физическое восстановление через XtraBackup требует, чтобы агент и MySQL выполнялись на одном хосте.");

        var realDatadir = ResolveRealPath(datadir);
        if (!string.Equals(realDatadir, datadir, StringComparison.Ordinal))
            _logger.LogInformation(
                "MySQL datadir '{DataDir}' resolves to real path '{RealPath}'",
                datadir, realDatadir);

        var (parent, _) = SplitPath(realDatadir);
        EnsureSameFsRename(parent, realDatadir);

        var serviceName = await DetectServiceNameAsync(connection, ct);
        if (serviceName is not null)
        {
            _logger.LogInformation(
                "MySQL is managed by service '{ServiceName}', will use service management for restore",
                serviceName);
        }
        else
        {
            await EnsureShutdownPrivilegeAsync(connection, ct);
            ResolveMysqld(connection);
        }
    }

    public Task ValidateRestoreSourceAsync(ConnectionConfig connection, string restoreFilePath, CancellationToken ct)
    {
        if (!File.Exists(restoreFilePath))
            throw new InvalidOperationException(
                $"Файл бэкапа '{restoreFilePath}' не найден на хосте агента.");
        return Task.CompletedTask;
    }

    public Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct) =>
        Task.CompletedTask;

    public async Task RestoreAsync(ConnectionConfig connection, string targetDatabase, string sourceDatabaseName, string restoreFilePath, CancellationToken ct)
    {
        EnsureSupportedOperatingSystem();

        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        var xbstream = _binaryResolver.Resolve(connection, "xbstream");
        var datadir = await QueryDataDirectoryAsync(connection, ct);

        if (!Directory.Exists(datadir))
            throw new RestorePermissionException(
                $"Каталог данных MySQL '{datadir}' недоступен на хосте агента.");

        var realDatadir = ResolveRealPath(datadir);
        var (parent, leaf) = SplitPath(realDatadir);

        CleanupOrphanStagingDirs(parent, leaf);

        var guid = Guid.NewGuid().ToString("N")[..8];
        var stagingPath = Path.Combine(parent, $"{leaf}.new-{guid}");
        var oldPath = Path.Combine(parent, $"{leaf}.old-{guid}");
        var failedPath = Path.Combine(parent, $"{leaf}.failed-{guid}");

        Directory.CreateDirectory(stagingPath);
        WriteMarkerFile(stagingPath);

        MysqlInstanceInfo instanceInfo;
        try
        {
            _logger.LogInformation("Extracting xbstream archive to staging '{StagingPath}'", stagingPath);
            await ExtractXbstreamAsync(xbstream, restoreFilePath, stagingPath, ct);

            _logger.LogInformation("Running xtrabackup --prepare on '{StagingPath}'", stagingPath);
            await PrepareBackupAsync(xtrabackup, stagingPath, ct);

            EnsureSameFsRename(parent, realDatadir);

            instanceInfo = await DetectInstanceInfoAsync(connection, realDatadir, ct);

            _logger.LogInformation("Stopping MySQL to swap datadir");
            await StopMysqlAsync(connection, instanceInfo, ct);
        }
        catch
        {
            TryDeleteDirectory(stagingPath);
            throw;
        }

        try
        {
            try
            {
                Directory.Move(realDatadir, oldPath);
                Directory.Move(stagingPath, realDatadir);

                await FixOwnershipAsync(realDatadir, instanceInfo, ct);

                _logger.LogInformation("Starting MySQL after restore");
                await StartMysqlAsync(connection, realDatadir, instanceInfo, ct);

                _logger.LogInformation("MySQL started successfully after physical restore");
                TryDeleteDirectory(oldPath);
            }
            catch (Exception swapException)
            {
                await RecoverAsync(connection, realDatadir, oldPath, stagingPath, failedPath, instanceInfo, swapException);

                if (swapException is OperationCanceledException)
                    throw;

                throw new InvalidOperationException(
                    $"Восстановление не удалось ({swapException.Message}). " +
                    "MySQL возвращён в исходное состояние.",
                    swapException);
            }
        }
        finally
        {
            if (instanceInfo.ServiceName is not null)
                await TryUnmaskServiceAsync(instanceInfo.ServiceName);
        }
    }

    private async Task ExtractXbstreamAsync(string xbstream, string archivePath, string targetDir, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = xbstream,
            Arguments = new[] { "-x", "-C", targetDir },
            RedirectStandardInput = true,
        };

        var result = await _processRunner.RunAsync(
            request,
            handleStdout: null,
            handleStdin: async (stdin, innerCt) =>
            {
                await using var fileStream = new FileStream(archivePath, FileMode.Open, FileAccess.Read,
                    FileShare.Read, bufferSize: 65536, useAsync: true);
                await using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
                await gzipStream.CopyToAsync(stdin.BaseStream, innerCt);
            },
            ct);

        if (result.ExitCode != 0)
        {
            var stderr = result.Stderr.Trim();
            throw new InvalidOperationException(
                $"xbstream завершился с ошибкой (код {result.ExitCode}): {stderr}");
        }
    }

    private static void EnsureSupportedOperatingSystem()
    {
        if (!OperatingSystem.IsLinux())
            throw new RestorePermissionException(
                "Физическое восстановление MySQL через Percona XtraBackup поддерживается только на Linux. " +
                "На Windows используйте logical-режим или запустите агента на Linux-хосте рядом с MySQL.");
    }

    private async Task PrepareBackupAsync(string xtrabackup, string targetDir, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = xtrabackup,
            Arguments = new[] { "--prepare", "--target-dir=" + targetDir },
        };

        var result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);

        if (result.ExitCode != 0)
        {
            var stderr = result.Stderr.Trim();
            throw new InvalidOperationException(
                $"xtrabackup --prepare завершился с ошибкой (код {result.ExitCode}): {stderr}");
        }

        _logger.LogInformation("xtrabackup --prepare completed successfully");
    }

    private async Task StopMysqlAsync(ConnectionConfig connection, MysqlInstanceInfo instanceInfo, CancellationToken ct)
    {
        if (instanceInfo.ServiceName is not null)
        {
            await StopServiceAsync(instanceInfo.ServiceName, instanceInfo.Pid, ct);
            if (!instanceInfo.Pid.HasValue)
                await WaitForPortClosedAsync(connection.Host, connection.Port, ct);
            return;
        }

        await using var conn = new MySqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand("SHUTDOWN;", conn);
        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.UnableToConnectToHost
                                        || ex.Number == 0
                                        || ex.Message.Contains("connection was lost", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation("MySQL SHUTDOWN issued — connection closed as expected");
        }

        await WaitForMysqlStopAsync(instanceInfo.Pid, connection, ct);
    }

    private async Task WaitForMysqlStopAsync(int? pid, ConnectionConfig connection, CancellationToken ct)
    {
        DateTime? startTime = pid.HasValue ? TryGetProcessStartTime(pid.Value) : null;
        if (pid.HasValue && startTime is null)
        {
            _logger.LogInformation("MySQL already stopped (PID {Pid} not found)", pid.Value);
            return;
        }

        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(1000, ct);

            if (pid.HasValue)
            {
                if (!IsSameProcessRunning(pid.Value, startTime!.Value))
                {
                    _logger.LogInformation("MySQL stopped (PID {Pid} exited)", pid.Value);
                    return;
                }
                continue;
            }

            try
            {
                await using var conn = new MySqlConnection(BuildConnectionString(connection));
                await conn.OpenAsync(ct);
            }
            catch (Exception ex) when (IsServerGoneException(ex))
            {
                _logger.LogInformation("MySQL stopped (connection refused)");
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Transient error while waiting for MySQL to stop — retrying");
            }
        }

        throw new InvalidOperationException(
            "MySQL не остановился в течение 60 секунд после SHUTDOWN. " +
            "Проверьте состояние процесса mysqld вручную.");
    }

    private static DateTime? TryGetProcessStartTime(int pid)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            return process.StartTime;
        }
        catch
        {
            return null;
        }
    }

    private static bool IsSameProcessRunning(int pid, DateTime expectedStartTime)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            if (process.HasExited) return false;
            return process.StartTime == expectedStartTime;
        }
        catch
        {
            return false;
        }
    }

    private static bool IsServerGoneException(Exception ex)
    {
        if (ex is SocketException { SocketErrorCode: SocketError.ConnectionRefused })
            return true;

        if (ex is MySqlException mysql)
        {
            if (mysql.ErrorCode == MySqlErrorCode.UnableToConnectToHost)
                return true;
            if (mysql.InnerException is SocketException { SocketErrorCode: SocketError.ConnectionRefused })
                return true;
        }

        return false;
    }

    private string ResolveMysqld(ConnectionConfig connection)
    {
        var xtrabackup = _binaryResolver.Resolve(connection, "xtrabackup");
        var mysqld = Path.Combine(Path.GetDirectoryName(xtrabackup) ?? string.Empty, "mysqld");

        if (!File.Exists(mysqld))
        {
            var pathMysqld = FindInPath("mysqld");
            if (pathMysqld is null)
                throw new RestorePermissionException(
                    "mysqld не найден на хосте агента. " +
                    "Укажите BinPath в конфигурации подключения или добавьте mysqld в PATH.");
            mysqld = pathMysqld;
        }

        return mysqld;
    }

    private async Task StartMysqlAsync(ConnectionConfig connection, string datadir, MysqlInstanceInfo instanceInfo, CancellationToken ct)
    {
        if (instanceInfo.ServiceName is not null)
        {
            await StartServiceAsync(instanceInfo.ServiceName, connection, ct);
            return;
        }

        var mysqld = ResolveMysqld(connection);

        var psi = new ProcessStartInfo
        {
            FileName = mysqld,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardInput = false,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
        };
        var keysToStrip = new HashSet<string>(SensitiveMysqldKeys, StringComparer.OrdinalIgnoreCase)
            { "--datadir", "--port", "--user" };
        foreach (var arg in FilterOriginalArgs(instanceInfo.OriginalArgs, keysToStrip))
            psi.ArgumentList.Add(arg);

        psi.ArgumentList.Add("--datadir=" + datadir);
        psi.ArgumentList.Add("--port=" + connection.Port);

        if (instanceInfo.OwnerUser is not null)
            psi.ArgumentList.Add("--user=" + instanceInfo.OwnerUser);

        using var process = new Process { StartInfo = psi };
        process.Start();

        _logger.LogInformation("mysqld started (PID {Pid}), waiting for it to accept connections on port {Port}",
            process.Id, connection.Port);

        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(120);
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(2000, ct);

                if (process.HasExited)
                    throw new InvalidOperationException(
                        $"mysqld завершился с кодом {process.ExitCode} во время запуска. " +
                        $"Логи смотрите в '{datadir}' (файл *.err).");

                if (await TryTcpConnectAsync(connection.Host, connection.Port, ct))
                {
                    _logger.LogInformation("MySQL accepting connections on port {Port}", connection.Port);
                    return;
                }
            }

            throw new InvalidOperationException(
                $"MySQL не начал принимать подключения в течение 120 секунд после запуска. " +
                $"Логи смотрите в '{datadir}' (файл *.err).");
        }
        catch
        {
            KillIfRunning(process);
            throw;
        }
    }

    private void KillIfRunning(Process process)
    {
        try
        {
            if (process.HasExited) return;
            process.Kill(entireProcessTree: true);
            _logger.LogWarning("Killed orphan mysqld process (PID {Pid}) after start failure", process.Id);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to kill mysqld process during cleanup");
        }
    }

    private static async Task<bool> TryTcpConnectAsync(string host, int port, CancellationToken ct)
    {
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(host, port, ct);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task WaitForPortClosedAsync(string host, int port, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            if (!await TryTcpConnectAsync(host, port, ct))
            {
                _logger.LogInformation("MySQL stopped (port {Port} no longer accepting connections)", port);
                return;
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"MySQL не остановился в течение 60 секунд (порт {port} всё ещё принимает подключения).");
    }

    private async Task FixOwnershipAsync(string newDatadir, MysqlInstanceInfo instanceInfo, CancellationToken ct)
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

    private async Task RecoverAsync(
        ConnectionConfig connection, string realDatadir, string oldPath, string stagingPath, string failedPath,
        MysqlInstanceInfo instanceInfo, Exception originalException)
    {
        _logger.LogError(originalException,
            "Restore swap failed at MySQL datadir '{DataDir}'. Attempting recovery.", realDatadir);

        try
        {
            var freshPid = await GetMysqlPidAsync(connection, CancellationToken.None);
            if (freshPid != instanceInfo.Pid)
            {
                _logger.LogInformation(
                    "Recovery: MySQL PID changed ({OldPid} -> {NewPid}), using fresh value",
                    instanceInfo.Pid, freshPid);
                instanceInfo = instanceInfo with { Pid = freshPid };
            }

            _logger.LogInformation("Stopping MySQL before recovery");
            await StopMysqlAsync(connection, instanceInfo, CancellationToken.None);
        }
        catch (Exception stopEx)
        {
            _logger.LogWarning(stopEx, "Failed to stop MySQL before recovery — it may already be stopped");
        }

        var datadirExists = Directory.Exists(realDatadir);
        var oldExists = Directory.Exists(oldPath);

        if (datadirExists && oldExists)
        {
            _logger.LogWarning("Both datadir and backup exist. Moving new to '{FailedPath}', restoring backup.", failedPath);

            if (!TryMoveDirectory(realDatadir, failedPath))
            {
                TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось убрать повреждённый каталог данных '{realDatadir}'. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }

            if (!TryMoveDirectory(oldPath, realDatadir))
            {
                TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось вернуть исходный каталог данных на место. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }
        }
        else if (oldExists && !datadirExists)
        {
            _logger.LogWarning("Datadir missing, restoring from '{OldPath}'", oldPath);

            if (!TryMoveDirectory(oldPath, realDatadir))
            {
                TryDeleteDirectory(stagingPath);
                throw new InvalidOperationException(
                    $"Не удалось вернуть исходный каталог данных на место. " +
                    $"Рабочие данные находятся в '{oldPath}'. " +
                    $"Переместите их в '{realDatadir}' вручную и запустите MySQL.",
                    originalException);
            }
        }
        else if (!datadirExists && !oldExists)
        {
            throw new InvalidOperationException(
                $"Каталог данных MySQL '{realDatadir}' и резервная копия '{oldPath}' оба отсутствуют. " +
                "Данные могут быть утеряны. Проверьте файловую систему.",
                originalException);
        }

        TryDeleteDirectory(stagingPath);

        if (Directory.Exists(realDatadir))
        {
            try
            {
                await StartMysqlAsync(connection, realDatadir, instanceInfo, CancellationToken.None);
                _logger.LogInformation("MySQL restarted on original datadir after restore failure");
            }
            catch (Exception startEx)
            {
                _logger.LogError(startEx, "Failed to start MySQL after rollback");
                throw new InvalidOperationException(
                    $"После отката datadir MySQL не запускается ({startEx.Message}). Запустите вручную.",
                    originalException);
            }
        }
    }

    private bool TryMoveDirectory(string from, string to)
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

    private async Task EnsureBinaryAvailableAsync(string binary, string name, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = binary,
            Arguments = new[] { "--version" },
        };

        ExternalProcessResult result;
        try
        {
            result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
        }
        catch (Exception ex)
        {
            throw new RestorePermissionException(
                $"Бинарник {name} недоступен на хосте агента ({ex.Message}). " +
                "Установите пакет percona-xtrabackup и убедитесь, что он находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
        }

        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"{binary} --version вернул код {result.ExitCode}. " +
                "Убедитесь, что percona-xtrabackup установлен и " + name + " находится в PATH " +
                "(или задайте ConnectionConfig.BinPath).");
    }

    private async Task<string> QueryDataDirectoryAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand("SELECT @@datadir;", conn);
        var result = await cmd.ExecuteScalarAsync(ct);
        var datadir = result as string ?? string.Empty;

        if (string.IsNullOrWhiteSpace(datadir))
            throw new RestorePermissionException(
                $"Не удалось получить datadir из MySQL-сервера подключения '{connection.Name}'.");

        return datadir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
    }

    private async Task EnsureShutdownPrivilegeAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new MySqlConnection(BuildConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new MySqlCommand(
            "SELECT COUNT(*) FROM information_schema.user_privileges " +
            "WHERE GRANTEE = CONCAT('''', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), '''@''', " +
            "SUBSTRING_INDEX(CURRENT_USER(), '@', -1), '''') " +
            "AND privilege_type IN ('SHUTDOWN', 'SUPER')", conn);

        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

        if (count == 0)
            throw new RestorePermissionException(
                $"Пользователь '{connection.Username}' подключения '{connection.Name}' " +
                $"не имеет привилегии SHUTDOWN, необходимой для физического восстановления MySQL. " +
                $"Выдайте привилегию: GRANT SHUTDOWN ON *.* TO '{connection.Username}'@'%'; FLUSH PRIVILEGES;");
    }

    private async Task<string?> DetectServiceNameAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var pid = await GetMysqlPidAsync(connection, ct);
        if (!pid.HasValue) return null;
        return await DetectSystemdUnitAsync(pid.Value, ct);
    }

    private async Task<string?> DetectSystemdUnitAsync(int pid, CancellationToken ct)
    {
        var cgroupFile = $"/proc/{pid}/cgroup";
        if (!File.Exists(cgroupFile)) return null;

        string content;
        try
        {
            content = await File.ReadAllTextAsync(cgroupFile, ct);
        }
        catch
        {
            return null;
        }

        var match = Regex.Match(content, @"system\.slice/([^\s/]+\.service)");
        if (!match.Success) return null;

        var unit = match.Groups[1].Value;
        _logger.LogInformation("Detected MySQL systemd unit: '{Unit}'", unit);
        return unit;
    }

    private async Task MaskServiceAsync(string serviceName, CancellationToken ct)
    {
        _logger.LogInformation("Masking MySQL service '{ServiceName}' to block systemd auto-restart", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("mask");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось замаскировать MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Восстановление прервано: без маскировки systemd может перезапустить MySQL во время подмены datadir." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));
    }

    private async Task TryUnmaskServiceAsync(string serviceName)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "systemctl",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.ArgumentList.Add("unmask");
            psi.ArgumentList.Add(serviceName);

            using var process = new Process { StartInfo = psi };
            process.Start();
            var stderrTask = process.StandardError.ReadToEndAsync();
            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await stderrTask;
            await stdoutTask;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to unmask MySQL service '{ServiceName}'", serviceName);
        }
    }

    private async Task StopServiceAsync(string serviceName, int? pid, CancellationToken ct)
    {
        await MaskServiceAsync(serviceName, ct);

        _logger.LogInformation("Stopping MySQL service '{ServiceName}'", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("stop");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось остановить MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом (запущен от root или настроен sudoers/polkit)." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));

        if (pid.HasValue)
        {
            var startTime = TryGetProcessStartTime(pid.Value);
            if (startTime is null)
            {
                _logger.LogInformation("MySQL service '{ServiceName}' already stopped (PID {Pid} not found)",
                    serviceName, pid.Value);
                return;
            }

            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (DateTime.UtcNow < deadline)
            {
                if (!IsSameProcessRunning(pid.Value, startTime.Value))
                {
                    _logger.LogInformation("MySQL service '{ServiceName}' stopped (PID {Pid} exited)",
                        serviceName, pid.Value);
                    return;
                }

                await Task.Delay(1000, ct);
            }

            throw new InvalidOperationException(
                $"MySQL-сервис '{serviceName}' не остановился в течение 60 секунд.");
        }

        _logger.LogInformation("MySQL service '{ServiceName}' stop command completed", serviceName);
    }

    private async Task StartServiceAsync(string serviceName, ConnectionConfig connection, CancellationToken ct)
    {
        await TryUnmaskServiceAsync(serviceName);

        _logger.LogInformation("Starting MySQL service '{ServiceName}'", serviceName);

        var psi = new ProcessStartInfo
        {
            FileName = "systemctl",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("start");
        psi.ArgumentList.Add(serviceName);

        using var process = new Process { StartInfo = psi };
        process.Start();
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        await stdoutTask;

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось запустить MySQL-сервис '{serviceName}' (код {process.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" Вывод: {stderr.Trim()}"));

        _logger.LogInformation("MySQL service start command completed, waiting for connections");

        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(2000, ct);

            if (!await IsServiceRunningAsync(serviceName, ct))
                throw new InvalidOperationException(
                    $"MySQL-сервис '{serviceName}' завершился во время запуска. " +
                    "Проверьте error log MySQL.");

            if (await TryTcpConnectAsync(connection.Host, connection.Port, ct))
            {
                _logger.LogInformation("MySQL service '{ServiceName}' started, accepting connections on port {Port}",
                    serviceName, connection.Port);
                return;
            }
        }

        throw new InvalidOperationException(
            $"MySQL-сервис '{serviceName}' запущен, но не начал принимать подключения в течение 120 секунд. " +
            "Проверьте error log MySQL.");
    }

    private async Task<bool> IsServiceRunningAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "systemctl",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.ArgumentList.Add("is-active");
            psi.ArgumentList.Add(serviceName);

            using var process = new Process { StartInfo = psi };
            process.Start();
            await process.StandardOutput.ReadToEndAsync(ct);
            await process.WaitForExitAsync(ct);

            return process.ExitCode == 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check if service '{ServiceName}' is running — assuming running", serviceName);
            return true;
        }
    }

    private async Task<MysqlInstanceInfo> DetectInstanceInfoAsync(
        ConnectionConfig connection, string datadir, CancellationToken ct)
    {
        IReadOnlyList<string> originalArgs = [];
        string? ownerUser = null;
        string? ownerGroup = null;
        string? serviceName = null;

        var pid = await GetMysqlPidAsync(connection, ct);

        if (pid.HasValue)
        {
            originalArgs = ReadProcessArgsFromProc(pid.Value);
            serviceName = await DetectSystemdUnitAsync(pid.Value, ct);
        }

        (ownerUser, ownerGroup) = ReadDirectoryOwner(datadir);

        return new MysqlInstanceInfo(originalArgs, pid, ownerUser, ownerGroup, serviceName);
    }

    private async Task<int?> GetMysqlPidAsync(ConnectionConfig connection, CancellationToken ct)
    {
        try
        {
            await using var conn = new MySqlConnection(BuildConnectionString(connection));
            await conn.OpenAsync(ct);

            await using var cmd = new MySqlCommand("SELECT @@pid_file;", conn);
            var pidFile = await cmd.ExecuteScalarAsync(ct) as string;

            if (string.IsNullOrWhiteSpace(pidFile) || !File.Exists(pidFile))
                return null;

            var pidContent = (await File.ReadAllTextAsync(pidFile, ct)).Trim();
            return int.TryParse(pidContent, out var pid) ? pid : null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to resolve MySQL PID");
            return null;
        }
    }

    private IReadOnlyList<string> ReadProcessArgsFromProc(int pid)
    {
        try
        {
            var cmdlineFile = $"/proc/{pid}/cmdline";
            if (!File.Exists(cmdlineFile)) return [];

            var raw = File.ReadAllBytes(cmdlineFile);
            var cmdline = Encoding.UTF8.GetString(raw);
            var allArgs = cmdline.Split('\0', StringSplitOptions.RemoveEmptyEntries);

            if (allArgs.Length == 0) return [];

            if (!allArgs[0].Contains("mysqld", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "PID {Pid} cmdline argv[0]='{Argv0}' does not look like mysqld — skipping arg capture (possible PID reuse)",
                    pid, allArgs[0]);
                return [];
            }

            if (allArgs.Length == 1) return [];

            var result = allArgs[1..];
            _logger.LogInformation("Captured {Count} original mysqld arguments from /proc/{Pid}", result.Length, pid);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to read mysqld args from /proc/{Pid}", pid);
            return [];
        }
    }

    internal static IReadOnlyList<string> FilterOriginalArgs(IReadOnlyList<string> args, ISet<string> keysToRemove)
    {
        var normalizedRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in keysToRemove)
            normalizedRemove.Add(NormalizeMysqldKey(key));

        var result = new List<string>(args.Count);
        for (var i = 0; i < args.Count; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal))
            {
                result.Add(arg);
                continue;
            }

            var eqIndex = arg.IndexOf('=');
            var rawKey = eqIndex >= 0 ? arg[..eqIndex] : arg;
            var normalizedKey = NormalizeMysqldKey(rawKey);

            if (!normalizedRemove.Contains(normalizedKey))
            {
                result.Add(arg);
                continue;
            }

            if (eqIndex < 0 && i + 1 < args.Count && !args[i + 1].StartsWith("-", StringComparison.Ordinal))
                i++;
        }
        return result;
    }

    private static string NormalizeMysqldKey(string key) => key.Replace('_', '-');

    private (string? user, string? group) ReadDirectoryOwner(string path)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "stat",
                ArgumentList = { "-c", "%U:%G", path },
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            using var process = new Process { StartInfo = psi };
            process.Start();
            var output = process.StandardOutput.ReadToEnd().Trim();

            if (!process.WaitForExit(5_000))
            {
                _logger.LogWarning("stat timed out after 5s for '{Path}' — killing process", path);
                try { process.Kill(entireProcessTree: true); } catch { }
                return (null, null);
            }

            if (process.ExitCode != 0 || string.IsNullOrEmpty(output))
                return (null, null);

            var parts = output.Split(':', 2);
            if (parts.Length != 2) return (null, null);

            _logger.LogInformation("Detected MySQL datadir owner: {User}:{Group}", parts[0], parts[1]);
            return (parts[0], parts[1]);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to detect datadir owner for '{Path}'", path);
            return (null, null);
        }
    }

    private string ResolveRealPath(string path)
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

    private void EnsureSameFsRename(string parent, string realPath)
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

    private static (string parent, string leaf) SplitPath(string path)
    {
        var trimmed = path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var parent = Path.GetDirectoryName(trimmed);
        var leaf = Path.GetFileName(trimmed);
        if (string.IsNullOrEmpty(parent) || string.IsNullOrEmpty(leaf))
            throw new InvalidOperationException(
                $"Не удалось разобрать путь '{path}' на родительский каталог и имя.");
        return (parent, leaf);
    }

    private static void WriteMarkerFile(string dir)
    {
        var path = Path.Combine(dir, MarkerFileName);
        File.WriteAllText(path, DateTime.UtcNow.ToString("o"));
    }

    private void CleanupOrphanStagingDirs(string parent, string leaf)
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

    private static string BuildConnectionString(ConnectionConfig connection) =>
        new MySqlConnectionStringBuilder
        {
            Server = connection.Host,
            Port = (uint)connection.Port,
            UserID = connection.Username,
            Password = connection.Password,
        }.ToString();

    private static string? FindInPath(string binary)
    {
        var pathVar = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrEmpty(pathVar)) return null;

        foreach (var dir in pathVar.Split(':', StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = Path.Combine(dir.Trim(), binary);
            if (File.Exists(candidate)) return candidate;
        }

        return null;
    }

    private void TryDeleteDirectory(string path)
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
