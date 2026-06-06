using System.Diagnostics;

namespace BackupsterAgent.Providers.Restore.Common;

public sealed class LinuxProcessInspector
{
    public DateTime? TryGetProcessStartTime(int pid)
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

    public bool IsSameProcessRunning(int pid, DateTime expectedStartTime)
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

    public async Task<bool> WaitForExitAsync(int? pid, TimeSpan timeout, CancellationToken ct)
    {
        if (!pid.HasValue) return true;

        var startTime = TryGetProcessStartTime(pid.Value);
        if (startTime is null) return true;

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (!IsSameProcessRunning(pid.Value, startTime.Value))
                return true;

            await Task.Delay(1000, ct);
        }

        return false;
    }
}
