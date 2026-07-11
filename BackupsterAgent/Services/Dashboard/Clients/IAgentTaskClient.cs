using BackupsterAgent.Contracts;

namespace BackupsterAgent.Services.Dashboard.Clients;

public interface IAgentTaskClient
{
    Task<AgentTaskForAgentDto?> FetchTaskAsync(
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);

    Task PatchTaskAsync(
        Guid taskId,
        PatchAgentTaskDto patch,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);

    Task ReportProgressAsync(
        Guid taskId,
        AgentTaskProgressDto progress,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null);
}
