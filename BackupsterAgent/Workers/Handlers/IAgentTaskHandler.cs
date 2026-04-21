using BackupsterAgent.Contracts;

namespace BackupsterAgent.Workers.Handlers;

public interface IAgentTaskHandler
{
    bool CanHandle(AgentTaskForAgentDto task);

    Task<PatchAgentTaskDto> HandleAsync(AgentTaskForAgentDto task, CancellationToken ct);
}
