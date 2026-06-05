using System.Diagnostics;
using System.Net.Sockets;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Resolvers;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlLifecycleManager : IMysqlLifecycleManager
{
    private readonly ILogger<MysqlLifecycleManager> _logger;
    private readonly MysqlServerProbe _probe;
    private readonly MysqlSystemdController _systemd;
    private readonly MysqlBinaryResolver _binaryResolver;

    public MysqlLifecycleManager(
        ILogger<MysqlLifecycleManager> logger,
        MysqlServerProbe probe,
        MysqlSystemdController systemd,
        MysqlBinaryResolver binaryResolver)
    {
        _logger = logger;
        _probe = probe;
        _systemd = systemd;
        _binaryResolver = binaryResolver;
    }

    public async Task StopMysqlAsync(ConnectionConfig connection, MysqlInstanceInfo instanceInfo, CancellationToken ct,
        bool unmaskServiceOnFailure = true)
    {
        if (instanceInfo.ServiceName is not null)
        {
            try
            {
                await StopServiceAsync(instanceInfo.ServiceName, instanceInfo.Pid, ct);
                if (!instanceInfo.Pid.HasValue)
                    await WaitForPortClosedAsync(connection.Host, connection.Port, ct);
            }
            catch when (unmaskServiceOnFailure)
            {
                _logger.LogWarning(
                    "Stopping MySQL service '{ServiceName}' failed after masking — unmasking to keep the service manageable",
                    instanceInfo.ServiceName);
                await _systemd.TryUnmaskAsync(instanceInfo.ServiceName);
                throw;
            }
            return;
        }

        await _probe.IssueShutdownAsync(connection, ct);

        await WaitForMysqlStopAsync(instanceInfo.Pid, connection, ct);
    }

    public async Task StartMysqlAsync(ConnectionConfig connection, string datadir, MysqlInstanceInfo instanceInfo, CancellationToken ct)
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
        var keysToStrip = new HashSet<string>(MysqldArgsSanitizer.SensitiveMysqldKeys, StringComparer.OrdinalIgnoreCase)
            { "--datadir", "--port", "--user" };
        foreach (var arg in MysqldArgsSanitizer.FilterOriginalArgs(instanceInfo.OriginalArgs, keysToStrip))
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

    public string ResolveMysqld(ConnectionConfig connection)
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

    public Task TryUnmaskServiceAsync(string serviceName) => _systemd.TryUnmaskAsync(serviceName);

    private async Task StopServiceAsync(string serviceName, int? pid, CancellationToken ct)
    {
        await _systemd.MaskAsync(serviceName);
        await _systemd.StopAsync(serviceName, ct);

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
        await _systemd.TryUnmaskAsync(serviceName);
        await _systemd.StartAsync(serviceName, ct);

        _logger.LogInformation("MySQL service start command completed, waiting for connections");

        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(2000, ct);

            if (!await _systemd.IsActiveAsync(serviceName, ct))
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

            if (await _probe.ProbeConnectionAsync(connection, ct) == MysqlConnectionProbeResult.ServerGone)
            {
                _logger.LogInformation("MySQL stopped (connection refused)");
                return;
            }
        }

        throw new InvalidOperationException(
            "MySQL не остановился в течение 60 секунд после SHUTDOWN. " +
            "Проверьте состояние процесса mysqld вручную.");
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
}
