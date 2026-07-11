using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Clients;
using BackupsterAgent.Workers;
using BackupsterAgent.Workers.Handlers;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Workers;

[TestFixture]
public sealed class AgentTaskPollingServiceTokenSnapshotTests
{
    [Test]
    public async Task ReceivedTask_UsesOneSnapshotThroughFinalPatch()
    {
        var task = new AgentTaskForAgentDto
        {
            Id = Guid.NewGuid(),
            Type = AgentTaskType.Delete,
            Delete = new DeleteTaskPayload(),
        };
        var client = new RecordingTaskClient(task);
        var handler = new RecordingHandler();
        var tokenProvider = new FakeDashboardTokenSnapshotProvider();
        using var service = new AgentTaskPollingService(
            client,
            tokenProvider,
            [handler],
            new NoopActivityLock(),
            NullLogger<AgentTaskPollingService>.Instance);

        await service.StartAsync(CancellationToken.None);
        await client.PatchStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(tokenProvider.CaptureCalls, Is.EqualTo(1));
            Assert.That(client.FetchTokenSnapshot, Is.SameAs(tokenProvider.Snapshot));
            Assert.That(handler.TokenSnapshot, Is.SameAs(tokenProvider.Snapshot));
            Assert.That(client.PatchTokenSnapshot, Is.SameAs(tokenProvider.Snapshot));
        });
    }

    private sealed class RecordingTaskClient(AgentTaskForAgentDto task) : IAgentTaskClient
    {
        public TaskCompletionSource PatchStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public DashboardTokenSnapshot? FetchTokenSnapshot { get; private set; }
        public DashboardTokenSnapshot? PatchTokenSnapshot { get; private set; }

        public Task<AgentTaskForAgentDto?> FetchTaskAsync(
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null)
        {
            FetchTokenSnapshot = tokenSnapshot;
            return Task.FromResult<AgentTaskForAgentDto?>(task);
        }

        public async Task PatchTaskAsync(
            Guid taskId,
            PatchAgentTaskDto patch,
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null)
        {
            PatchTokenSnapshot = tokenSnapshot;
            PatchStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }

        public Task ReportProgressAsync(
            Guid taskId,
            AgentTaskProgressDto progress,
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null) =>
            throw new NotSupportedException();
    }

    private sealed class RecordingHandler : IAgentTaskHandler
    {
        public DashboardTokenSnapshot? TokenSnapshot { get; private set; }

        public bool CanHandle(AgentTaskForAgentDto task) => true;

        public Task<PatchAgentTaskDto> HandleAsync(
            AgentTaskForAgentDto task,
            DashboardTokenSnapshot tokenSnapshot,
            CancellationToken ct)
        {
            TokenSnapshot = tokenSnapshot;
            return Task.FromResult(new PatchAgentTaskDto { Status = AgentTaskStatus.Success });
        }
    }

    private sealed class NoopActivityLock : IAgentActivityLock
    {
        public Task<IDisposable> AcquireAsync(string activityName, CancellationToken ct) =>
            Task.FromResult<IDisposable>(new Handle());

        private sealed class Handle : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
