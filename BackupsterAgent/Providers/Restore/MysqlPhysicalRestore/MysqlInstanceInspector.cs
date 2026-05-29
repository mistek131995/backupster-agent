using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed class MysqlInstanceInspector
{
    private readonly ILogger<MysqlInstanceInspector> _logger;
    private readonly MysqlServerProbe _probe;

    public MysqlInstanceInspector(
        ILogger<MysqlInstanceInspector> logger,
        MysqlServerProbe probe)
    {
        _logger = logger;
        _probe = probe;
    }

    public async Task<MysqlInstanceInfo> DetectInstanceInfoAsync(
        ConnectionConfig connection, string datadir, CancellationToken ct)
    {
        IReadOnlyList<string> originalArgs = [];
        string? ownerUser = null;
        string? ownerGroup = null;
        string? serviceName = null;

        var pid = await _probe.GetMysqlPidAsync(connection, ct);

        if (pid.HasValue)
        {
            originalArgs = ReadProcessArgsFromProc(pid.Value);
            serviceName = await DetectSystemdUnitAsync(pid.Value, ct);
        }

        (ownerUser, ownerGroup) = ReadDirectoryOwner(datadir);

        return new MysqlInstanceInfo(originalArgs, pid, ownerUser, ownerGroup, serviceName);
    }

    public async Task<string?> DetectServiceNameAsync(ConnectionConfig connection, CancellationToken ct)
    {
        var pid = await _probe.GetMysqlPidAsync(connection, ct);
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
}
