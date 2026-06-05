using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Processes;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

public sealed class PostgresClusterLifecycle
{
    private readonly ILogger<PostgresClusterLifecycle> _logger;
    private readonly IExternalProcessRunner _processRunner;
    private readonly RestoreSettings _restoreSettings;

    public PostgresClusterLifecycle(
        ILogger<PostgresClusterLifecycle> logger,
        IExternalProcessRunner processRunner,
        IOptions<RestoreSettings> restoreSettings)
    {
        _logger = logger;
        _processRunner = processRunner;
        _restoreSettings = restoreSettings.Value;
    }

    internal async Task<PostgresClusterControl> DetectAsync(string pgDataPath, CancellationToken ct)
    {
        var pid = await TryReadPostmasterPidAsync(pgDataPath, ct);
        if (pid is null)
            return new PostgresClusterControl(PostgresClusterControlKind.Unmanaged, null, null, null);

        if (OperatingSystem.IsLinux())
        {
            var unit = await DetectSystemdUnitAsync(pid.Value, ct);
            if (unit is null)
                return new PostgresClusterControl(PostgresClusterControlKind.Unmanaged, null, null, pid);

            await EnsureSystemdUnitOwnsPostmasterAsync(unit, pid.Value, ct);
            _logger.LogInformation("Detected PostgreSQL systemd unit '{Unit}' for PID {Pid}", unit, pid);
            return new PostgresClusterControl(PostgresClusterControlKind.Systemd, unit, null, pid);
        }

        if (OperatingSystem.IsWindows())
        {
            var service = await DetectWindowsServiceAsync(pid.Value, ct);
            if (service is null)
                return new PostgresClusterControl(PostgresClusterControlKind.Unmanaged, null, null, pid);

            _logger.LogInformation(
                "Detected PostgreSQL Windows service '{ServiceName}' for PID {Pid}",
                service.Name, pid);
            return new PostgresClusterControl(
                PostgresClusterControlKind.WindowsService, service.Name, NormalizeWindowsServiceAccount(service.StartName), pid);
        }

        return new PostgresClusterControl(PostgresClusterControlKind.Unmanaged, null, null, pid);
    }

    internal async Task PrepareStagingPermissionsAsync(
        PostgresClusterControl control, string sourcePgDataPath, string stagingPath, CancellationToken ct)
    {
        if (OperatingSystem.IsLinux())
        {
            var sourceOwner = await TryGetLinuxOwnerAsync(sourcePgDataPath, ct);
            if (sourceOwner is not null)
            {
                var stagingOwner = await TryGetLinuxOwnerAsync(stagingPath, ct);
                if (stagingOwner != sourceOwner)
                    await ChownRecursiveAsync(stagingPath, sourceOwner, ct);
            }

            var sourceMode = await TryGetLinuxModeAsync(sourcePgDataPath, ct);
            if (sourceMode is not null)
                await ChmodAsync(stagingPath, sourceMode, ct);
        }
        else if (OperatingSystem.IsWindows() &&
                 control.Kind == PostgresClusterControlKind.WindowsService &&
                 !string.IsNullOrWhiteSpace(control.ServiceAccount))
        {
            await GrantWindowsServiceAccountAsync(stagingPath, control.ServiceAccount, ct);
        }
    }

    internal async Task StopAsync(PostgresClusterControl control, string pgCtl, string pgDataPath, CancellationToken ct)
    {
        switch (control.Kind)
        {
            case PostgresClusterControlKind.Systemd:
                await StopSystemdAsync(control.ServiceName!, ct);
                await WaitForPidExitAsync(control.PostmasterPid, TimeSpan.FromSeconds(60), ct);
                return;
            case PostgresClusterControlKind.WindowsService:
                await ControlWindowsServiceAsync(control.ServiceName!, "Stop-Service", "Stopped", ct);
                await WaitForPidExitAsync(control.PostmasterPid, TimeSpan.FromSeconds(60), ct);
                return;
            default:
                await RunPgCtlAsync(pgCtl, ["stop", "-D", pgDataPath, "-m", "fast", "-w"], ct);
                return;
        }
    }

    internal async Task StartAsync(
        PostgresClusterControl control, string pgCtl, string pgDataPath, string startLog, CancellationToken ct)
    {
        switch (control.Kind)
        {
            case PostgresClusterControlKind.Systemd:
                await StartSystemdAsync(control.ServiceName!, ct);
                return;
            case PostgresClusterControlKind.WindowsService:
                await ControlWindowsServiceAsync(control.ServiceName!, "Start-Service", "Running", ct);
                return;
            default:
                await StartWithPgCtlAsync(pgCtl, pgDataPath, startLog, ct);
                return;
        }
    }

    internal async Task TryStopForRecoveryAsync(
        PostgresClusterControl control, string pgCtl, string pgDataPath, string realPgDataPath)
    {
        try
        {
            switch (control.Kind)
            {
                case PostgresClusterControlKind.Systemd:
                    await StopSystemdAsync(control.ServiceName!, CancellationToken.None);
                    break;
                case PostgresClusterControlKind.WindowsService:
                    await ControlWindowsServiceAsync(control.ServiceName!, "Stop-Service", "Stopped", CancellationToken.None);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to stop PostgreSQL service during recovery");
        }

        await TryStopOrphanedPostmasterAsync(pgCtl, pgDataPath);

        if (!string.Equals(pgDataPath, realPgDataPath, StringComparison.Ordinal))
            await TryStopOrphanedPostmasterAsync(pgCtl, realPgDataPath);
    }

    internal string BuildManualStartInstruction(PostgresClusterControl control, string pgDataPath) =>
        control.Kind switch
        {
            PostgresClusterControlKind.Systemd => $"systemctl start {control.ServiceName}",
            PostgresClusterControlKind.WindowsService => $"Start-Service '{control.ServiceName}'",
            _ => $"pg_ctl start -D \"{pgDataPath}\"",
        };

    internal async Task<string> CollectStartDiagnosticsAsync(
        PostgresClusterControl control, string startLog, CancellationToken ct)
    {
        if (control.Kind == PostgresClusterControlKind.Systemd)
            return await TryCollectJournalAsync(control.ServiceName!, ct);

        if (control.Kind == PostgresClusterControlKind.WindowsService)
            return await TryCollectWindowsServiceStatusAsync(control.ServiceName!, ct);

        return TryReadTextFile(startLog);
    }

    internal static string? TryParseSystemdUnit(string cgroupContent)
    {
        var matches = Regex.Matches(cgroupContent, @"(?:^|/)([^/\s]+\.service)(?=$|/|\s)");
        if (matches.Count == 0) return null;

        var services = matches
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var postgresLike = services
            .Where(IsPostgresServiceName)
            .ToArray();

        if (postgresLike.Length == 1)
            return postgresLike[0];

        return services.Length == 1 ? services[0] : null;
    }

    private async Task<int?> TryReadPostmasterPidAsync(string pgDataPath, CancellationToken ct)
    {
        var pidFile = Path.Combine(pgDataPath, "postmaster.pid");
        if (!File.Exists(pidFile))
        {
            _logger.LogWarning(
                "postmaster.pid not found in '{PgDataPath}' — using unmanaged PostgreSQL control", pgDataPath);
            return null;
        }

        try
        {
            var firstLine = (await File.ReadAllLinesAsync(pidFile, ct)).FirstOrDefault();
            if (int.TryParse(firstLine?.Trim(), out var pid))
                return pid;

            _logger.LogWarning(
                "Failed to parse postmaster PID from '{PidFile}' (first line: '{Line}') — using unmanaged PostgreSQL control",
                pidFile, firstLine);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to read '{PidFile}' — using unmanaged PostgreSQL control", pidFile);
            return null;
        }
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
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read '{CgroupFile}' — using unmanaged PostgreSQL control", cgroupFile);
            return null;
        }

        return TryParseSystemdUnit(content);
    }

    private async Task EnsureSystemdUnitOwnsPostmasterAsync(string unit, int postmasterPid, CancellationToken ct)
    {
        var result = await RunSystemctlAsync("show", [unit, "--property", "MainPID", "--value"], ct,
            _restoreSettings.SystemctlTimeoutSeconds);

        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"Не удалось проверить systemd-юнит PostgreSQL '{unit}' (код {result.ExitCode}). " +
                "Physical restore не будет выполнять swap без однозначной связи systemd-юнита с postmaster." +
                FormatOutput(result));

        var raw = result.Stdout.Trim();
        if (!int.TryParse(raw, out var mainPid) || mainPid != postmasterPid)
            throw new RestorePermissionException(
                $"Небезопасно управлять PostgreSQL через systemd-юнит '{unit}': MainPID={raw}, postmaster.pid={postmasterPid}. " +
                "Physical restore не будет выполнять swap, если service manager не указывает ровно на этот кластер. " +
                "Запустите кластер вручную через pg_ctl или настройте отдельный systemd-юнит для target-кластера.");
    }

    private async Task<WindowsServiceInfo?> DetectWindowsServiceAsync(int pid, CancellationToken ct)
    {
        var script =
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; " +
            $"Get-CimInstance Win32_Service -Filter 'ProcessId={pid}' | " +
            "Select-Object -First 1 -Property Name,StartName | ConvertTo-Json -Compress";

        var result = await RunPowerShellAsync(script, ct, _restoreSettings.SystemctlTimeoutSeconds);
        if (result.ExitCode != 0 || string.IsNullOrWhiteSpace(result.Stdout))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(result.Stdout);
            if (!doc.RootElement.TryGetProperty("Name", out var nameElement)) return null;

            var name = nameElement.GetString();
            if (string.IsNullOrWhiteSpace(name)) return null;

            var startName = doc.RootElement.TryGetProperty("StartName", out var startNameElement)
                ? startNameElement.GetString() ?? string.Empty
                : string.Empty;

            return new WindowsServiceInfo(name, startName);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse Windows service metadata for PostgreSQL PID {Pid}", pid);
            return null;
        }
    }

    private async Task StopSystemdAsync(string unit, CancellationToken ct)
    {
        _logger.LogInformation("Stopping PostgreSQL systemd unit '{Unit}'", unit);

        var result = await RunSystemctlAsync("stop", [unit], ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось остановить systemd-юнит PostgreSQL '{unit}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом (root или настроенный polkit/sudoers)." +
                FormatOutput(result));
    }

    private async Task StartSystemdAsync(string unit, CancellationToken ct)
    {
        _logger.LogInformation("Starting PostgreSQL systemd unit '{Unit}'", unit);

        var result = await RunSystemctlAsync("start", [unit], ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);
        if (result.ExitCode != 0)
        {
            var journal = await TryCollectJournalAsync(unit, CancellationToken.None);
            throw new InvalidOperationException(
                $"Не удалось запустить systemd-юнит PostgreSQL '{unit}' (код {result.ExitCode}). " +
                "Убедитесь, что агент имеет права на управление сервисом и что восстановленный PGDATA доступен пользователю PostgreSQL." +
                FormatOutput(result) +
                (string.IsNullOrWhiteSpace(journal) ? "" : $" Диагностика systemd: {journal}"));
        }
    }

    private async Task ControlWindowsServiceAsync(
        string serviceName, string command, string targetStatus, CancellationToken ct)
    {
        var timeoutSeconds = Math.Max(_restoreSettings.SystemctlStopStartTimeoutSeconds, 1);
        var script =
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; " +
            "$ErrorActionPreference = 'Stop'; " +
            $"{command} -Name {PowerShellQuote(serviceName)}; " +
            $"(Get-Service -Name {PowerShellQuote(serviceName)}).WaitForStatus('{targetStatus}', [TimeSpan]::FromSeconds({timeoutSeconds})); " +
            $"(Get-Service -Name {PowerShellQuote(serviceName)}).Status";

        _logger.LogInformation(
            "{Command} PostgreSQL Windows service '{ServiceName}'",
            command.StartsWith("Stop", StringComparison.Ordinal) ? "Stopping" : "Starting",
            serviceName);

        var result = await RunPowerShellAsync(script, ct, timeoutSeconds);
        if (result.ExitCode != 0)
            throw new InvalidOperationException(
                $"Не удалось изменить состояние Windows-сервиса PostgreSQL '{serviceName}' на '{targetStatus}' (код {result.ExitCode}). " +
                "Убедитесь, что агент запущен с правами на управление сервисом." +
                FormatOutput(result));
    }

    private async Task ChownRecursiveAsync(string stagingPath, string owner, CancellationToken ct)
    {
        var timeoutSeconds = Math.Max(_restoreSettings.ChownTimeoutSeconds, 1);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        var request = new ExternalProcessRequest
        {
            FileName = "chown",
            Arguments = new[] { "-R", owner, stagingPath },
        };

        ExternalProcessResult result;
        try
        {
            result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Команда 'chown -R {owner} {stagingPath}' не завершилась за {timeoutSeconds} секунд.");
        }

        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"Не удалось назначить владельца '{owner}' для staging-каталога PostgreSQL '{stagingPath}' (код {result.ExitCode}). " +
                "Для managed restore агент должен иметь права на смену владельца PGDATA." +
                FormatOutput(result));

        _logger.LogInformation("Staging PostgreSQL PGDATA owner set to '{Owner}'", owner);
    }

    private async Task GrantWindowsServiceAccountAsync(string stagingPath, string serviceAccount, CancellationToken ct)
    {
        var request = new ExternalProcessRequest
        {
            FileName = "icacls",
            Arguments = new[] { stagingPath, "/grant", $"{serviceAccount}:(OI)(CI)F", "/T", "/C" },
        };

        var result = await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, ct);
        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"Не удалось выдать Windows-сервису PostgreSQL '{serviceAccount}' доступ к staging-каталогу '{stagingPath}' (код {result.ExitCode}). " +
                "Запустите агент с правами администратора или настройте ACL вручную." +
                FormatOutput(result));

        _logger.LogInformation(
            "Granted PostgreSQL service account '{ServiceAccount}' access to staging PGDATA",
            serviceAccount);
    }

    private async Task<string?> TryGetLinuxOwnerAsync(string path, CancellationToken ct)
    {
        var result = await RunExternalAsync("stat", ["-c", "%u:%g", path], ct, _restoreSettings.SystemctlTimeoutSeconds);
        if (result.ExitCode != 0)
        {
            _logger.LogWarning(
                "Failed to read Linux owner for '{Path}' (exit code {ExitCode}).{Output}",
                path, result.ExitCode, FormatOutput(result));
            return null;
        }

        var owner = result.Stdout.Trim();
        return string.IsNullOrWhiteSpace(owner) ? null : owner;
    }

    private async Task<string?> TryGetLinuxModeAsync(string path, CancellationToken ct)
    {
        var result = await RunExternalAsync("stat", ["-c", "%a", path], ct, _restoreSettings.SystemctlTimeoutSeconds);
        if (result.ExitCode != 0)
        {
            _logger.LogWarning(
                "Failed to read Linux mode for '{Path}' (exit code {ExitCode}).{Output}",
                path, result.ExitCode, FormatOutput(result));
            return null;
        }

        var mode = result.Stdout.Trim();
        return string.IsNullOrWhiteSpace(mode) ? null : mode;
    }

    private async Task ChmodAsync(string stagingPath, string mode, CancellationToken ct)
    {
        var result = await RunExternalAsync("chmod", [mode, stagingPath], ct, _restoreSettings.SystemctlTimeoutSeconds);
        if (result.ExitCode != 0)
            throw new RestorePermissionException(
                $"Не удалось назначить права '{mode}' для staging-каталога PostgreSQL '{stagingPath}' (код {result.ExitCode}). " +
                "Для physical restore агент должен иметь права на изменение прав PGDATA." +
                FormatOutput(result));

        _logger.LogInformation("Staging PostgreSQL PGDATA mode set to '{Mode}'", mode);
    }

    private async Task RunPgCtlAsync(string pgCtl, string[] args, CancellationToken ct)
    {
        var result = await RunExternalAsync(pgCtl, args, ct, _restoreSettings.SystemctlStopStartTimeoutSeconds);
        if (result.ExitCode != 0)
        {
            var detail = string.IsNullOrWhiteSpace(result.Stderr) ? result.Stdout.Trim() : result.Stderr.Trim();
            _logger.LogError("pg_ctl failed. ExitCode: {ExitCode}. Detail: {Detail}", result.ExitCode, detail);
            throw new InvalidOperationException($"pg_ctl завершился с кодом {result.ExitCode}: {detail}");
        }

        if (!string.IsNullOrWhiteSpace(result.Stdout))
            _logger.LogDebug("pg_ctl stdout: {Output}", result.Stdout.Trim());
    }

    private async Task StartWithPgCtlAsync(string pgCtl, string pgDataPath, string startLog, CancellationToken ct)
    {
        var timeoutSeconds = Math.Max(_restoreSettings.PgCtlStartTimeoutSeconds, 1);
        var result = await RunExternalAsync(
            pgCtl,
            ["start", "-D", pgDataPath, "-l", startLog, "-w", "-t", timeoutSeconds.ToString()],
            ct,
            timeoutSeconds);

        if (result.ExitCode != 0)
        {
            var logContents = TryReadTextFile(startLog);
            throw new InvalidOperationException(
                $"pg_ctl start завершился с кодом {result.ExitCode} (таймаут: {timeoutSeconds}с). " +
                $"Лог сервера: {logContents}");
        }
    }

    private async Task TryStopOrphanedPostmasterAsync(string pgCtl, string pgDataPath)
    {
        try
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await RunPgCtlAsync(pgCtl, ["stop", "-D", pgDataPath, "-m", "immediate", "-w", "-t", "60"], stopCts.Token);
            _logger.LogInformation("Orphaned postmaster at '{PgDataPath}' stopped", pgDataPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to stop orphaned postmaster at '{PgDataPath}' — rollback may fail if postmaster is still holding files",
                pgDataPath);
        }
    }

    private async Task WaitForPidExitAsync(int? pid, TimeSpan timeout, CancellationToken ct)
    {
        if (!pid.HasValue) return;

        var startTime = TryGetProcessStartTime(pid.Value);
        if (startTime is null) return;

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (!IsSameProcessRunning(pid.Value, startTime.Value))
                return;

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"PostgreSQL postmaster PID {pid.Value} не завершился в течение {timeout.TotalSeconds:0} секунд после остановки сервиса.");
    }

    private async Task<ExternalProcessResult> RunSystemctlAsync(
        string verb, string[] arguments, CancellationToken ct, int timeoutSeconds)
    {
        var args = new[] { verb }.Concat(arguments).ToArray();
        return await RunExternalAsync("systemctl", args, ct, timeoutSeconds);
    }

    private async Task<ExternalProcessResult> RunPowerShellAsync(string script, CancellationToken ct, int timeoutSeconds) =>
        await RunExternalAsync(
            "powershell",
            ["-NoProfile", "-NonInteractive", "-Command", script],
            ct,
            timeoutSeconds);

    private async Task<ExternalProcessResult> RunExternalAsync(
        string fileName, string[] arguments, CancellationToken ct, int timeoutSeconds)
    {
        timeoutSeconds = Math.Max(timeoutSeconds, 1);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        var request = new ExternalProcessRequest
        {
            FileName = fileName,
            Arguments = arguments,
            EnvironmentOverrides = new Dictionary<string, string?>
            {
                ["LC_MESSAGES"] = "C",
                ["LANG"] = "C",
            },
        };

        try
        {
            return await _processRunner.RunAsync(request, handleStdout: null, handleStdin: null, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Команда '{fileName} {string.Join(" ", arguments)}' не завершилась за {timeoutSeconds} секунд.");
        }
    }

    private async Task<string> TryCollectJournalAsync(string unit, CancellationToken ct)
    {
        try
        {
            var result = await RunExternalAsync(
                "journalctl",
                ["-u", unit, "-n", "80", "--no-pager"],
                ct,
                _restoreSettings.SystemctlTimeoutSeconds);

            var text = string.IsNullOrWhiteSpace(result.Stdout) ? result.Stderr : result.Stdout;
            return Truncate(text.Trim(), 4000);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to collect journalctl diagnostics for '{Unit}'", unit);
            return string.Empty;
        }
    }

    private async Task<string> TryCollectWindowsServiceStatusAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var script =
                "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; " +
                $"Get-CimInstance Win32_Service -Filter \"Name='{EscapeWmiLiteral(serviceName)}'\" | " +
                "Select-Object Name,State,Status,ProcessId,StartName,ExitCode,ServiceSpecificExitCode | Format-List | Out-String";

            var result = await RunPowerShellAsync(script, ct, _restoreSettings.SystemctlTimeoutSeconds);
            var text = string.IsNullOrWhiteSpace(result.Stdout) ? result.Stderr : result.Stdout;
            return Truncate(text.Trim(), 4000);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to collect Windows service diagnostics for '{ServiceName}'", serviceName);
            return string.Empty;
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

    private static string TryReadTextFile(string path)
    {
        try
        {
            if (!File.Exists(path)) return "(log file not found)";
            var content = File.ReadAllText(path).Trim();
            return string.IsNullOrEmpty(content) ? "(empty)" : content;
        }
        catch (Exception ex)
        {
            return $"(failed to read: {ex.Message})";
        }
    }

    private static bool IsPostgresServiceName(string serviceName) =>
        serviceName.Contains("postgres", StringComparison.OrdinalIgnoreCase) ||
        serviceName.Contains("pgsql", StringComparison.OrdinalIgnoreCase) ||
        serviceName.Contains("edb-as", StringComparison.OrdinalIgnoreCase);

    private static string NormalizeWindowsServiceAccount(string? account)
    {
        if (string.IsNullOrWhiteSpace(account)) return string.Empty;

        return account.Trim() switch
        {
            "LocalSystem" => "NT AUTHORITY\\SYSTEM",
            "LocalService" => "NT AUTHORITY\\LOCAL SERVICE",
            "NetworkService" => "NT AUTHORITY\\NETWORK SERVICE",
            var value => value,
        };
    }

    private static string PowerShellQuote(string value) =>
        "'" + value.Replace("'", "''") + "'";

    private static string EscapeWmiLiteral(string value) =>
        value.Replace("\\", "\\\\").Replace("'", "\\'");

    private static string FormatOutput(ExternalProcessResult result)
    {
        var output = string.IsNullOrWhiteSpace(result.Stderr) ? result.Stdout : result.Stderr;
        return string.IsNullOrWhiteSpace(output) ? string.Empty : $" Вывод: {Truncate(output.Trim(), 2000)}";
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength] + "...";

    private sealed record WindowsServiceInfo(string Name, string StartName);
}
