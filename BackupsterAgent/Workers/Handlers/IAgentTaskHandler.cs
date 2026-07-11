using BackupsterAgent.Contracts;
using BackupsterAgent.Services.Dashboard;

namespace BackupsterAgent.Workers.Handlers;

public interface IAgentTaskHandler
{
    bool CanHandle(AgentTaskForAgentDto task);

    Task<PatchAgentTaskDto> HandleAsync(
        AgentTaskForAgentDto task,
        DashboardTokenSnapshot tokenSnapshot,
        CancellationToken ct);
}
