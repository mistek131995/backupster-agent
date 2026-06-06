using System.Text.RegularExpressions;

namespace BackupsterAgent.Providers.Restore.Common;

public sealed class SystemdUnitDetector
{
    private readonly ILogger<SystemdUnitDetector> _logger;

    public SystemdUnitDetector(ILogger<SystemdUnitDetector> logger)
    {
        _logger = logger;
    }

    public async Task<string?> DetectUnitAsync(
        int pid,
        Func<string, bool>? preferredServiceName,
        string subject,
        CancellationToken ct)
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
            _logger.LogWarning(ex,
                "Failed to read '{CgroupFile}' for {Subject} PID {Pid}", cgroupFile, subject, pid);
            return null;
        }

        var unit = TryParseSystemdUnit(content, preferredServiceName);
        if (unit is not null)
            _logger.LogInformation("Detected systemd unit '{Unit}' for {Subject} PID {Pid}", unit, subject, pid);

        return unit;
    }

    public static string? TryParseSystemdUnit(string cgroupContent, Func<string, bool>? preferredServiceName = null)
    {
        var matches = Regex.Matches(cgroupContent, @"(?:^|/)([^/\s]+\.service)(?=$|/|\s)");
        if (matches.Count == 0) return null;

        var services = matches
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (preferredServiceName is not null)
        {
            var preferred = services
                .Where(preferredServiceName)
                .ToArray();

            if (preferred.Length == 1)
                return preferred[0];
        }

        return services.Length == 1 ? services[0] : null;
    }
}
