namespace DbBackupAgent.Services.Common;

public interface IAgentActivityLock
{
    Task<IDisposable> AcquireAsync(string activityName, CancellationToken ct);
}
