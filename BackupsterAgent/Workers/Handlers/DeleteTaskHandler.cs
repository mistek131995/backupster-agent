using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Delete;

namespace BackupsterAgent.Workers.Handlers;

public sealed class DeleteTaskHandler : IAgentTaskHandler
{
    private readonly BackupDeleteService _backupDelete;
    private readonly IProgressReporterFactory _reporterFactory;
    private readonly ILogger<DeleteTaskHandler> _logger;

    public DeleteTaskHandler(
        BackupDeleteService backupDelete,
        IProgressReporterFactory reporterFactory,
        ILogger<DeleteTaskHandler> logger)
    {
        _backupDelete = backupDelete;
        _reporterFactory = reporterFactory;
        _logger = logger;
    }

    public bool CanHandle(AgentTaskForAgentDto task) => task.Type == AgentTaskType.Delete;

    public async Task<PatchAgentTaskDto> HandleAsync(AgentTaskForAgentDto task, CancellationToken ct)
    {
        if (task.Delete is null)
        {
            _logger.LogWarning(
                "DeleteTaskHandler: delete task {TaskId} has empty payload.", task.Id);
            return new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = "Сервер не передал тело delete-задачи.",
            };
        }

        var payload = task.Delete;

        _logger.LogInformation(
            "DeleteTaskHandler: executing delete task {TaskId} (storage '{Storage}')",
            task.Id, payload.StorageName);

        await using var reporter = _reporterFactory.CreateForDelete(task.Id);

        var result = await _backupDelete.RunAsync(task.Id, payload, reporter, ct);

        return result.IsSuccess
            ? new PatchAgentTaskDto { Status = AgentTaskStatus.Success }
            : new PatchAgentTaskDto
            {
                Status = AgentTaskStatus.Failed,
                ErrorMessage = result.ErrorMessage,
            };
    }
}
