using BackupsterAgent.Contracts;

namespace BackupsterAgent.Services.Dashboard.Clients;

public interface IAgentTaskClient
{
    Task<AgentTaskForAgentDto?> FetchTaskAsync(CancellationToken ct);

    Task PatchTaskAsync(Guid taskId, PatchAgentTaskDto patch, CancellationToken ct);

    Task ReportProgressAsync(Guid taskId, AgentTaskProgressDto progress, CancellationToken ct);
}
