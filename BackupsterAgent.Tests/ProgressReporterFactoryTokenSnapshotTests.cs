using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Clients;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests;

[TestFixture]
public sealed class ProgressReporterFactoryTokenSnapshotTests
{
    [Test]
    public async Task RestoreProgress_UsesOperationTokenSnapshot()
    {
        var taskClient = new RecordingTaskClient();
        var factory = new ProgressReporterFactory(
            taskClient, new FakeBackupRecordClient(), NullLoggerFactory.Instance);
        var snapshot = new DashboardTokenSnapshot("restore-token");

        await using var reporter = factory.CreateForRestore(Guid.NewGuid(), snapshot);
        reporter.Report(RestoreStage.DownloadingDump);

        var actual = await taskClient.ProgressSnapshot.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(actual, Is.SameAs(snapshot));
    }

    [Test]
    public async Task DeleteProgress_UsesOperationTokenSnapshot()
    {
        var taskClient = new RecordingTaskClient();
        var factory = new ProgressReporterFactory(
            taskClient, new FakeBackupRecordClient(), NullLoggerFactory.Instance);
        var snapshot = new DashboardTokenSnapshot("delete-token");

        await using var reporter = factory.CreateForDelete(Guid.NewGuid(), snapshot);
        reporter.Report(DeleteStage.DeletingDump);

        var actual = await taskClient.ProgressSnapshot.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(actual, Is.SameAs(snapshot));
    }

    [Test]
    public async Task BackupProgress_UsesOperationTokenSnapshot()
    {
        var recordClient = new FakeBackupRecordClient();
        var factory = new ProgressReporterFactory(
            new RecordingTaskClient(), recordClient, NullLoggerFactory.Instance);
        var snapshot = new DashboardTokenSnapshot("backup-token");

        await using var reporter = factory.CreateForBackup(Guid.NewGuid(), snapshot);
        reporter.Report(BackupStage.Dumping);

        await WaitUntilAsync(() => recordClient.ProgressCalls == 1);
        Assert.That(recordClient.LastProgressTokenSnapshot, Is.SameAs(snapshot));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(10, cts.Token);
    }

    private sealed class RecordingTaskClient : IAgentTaskClient
    {
        public TaskCompletionSource<DashboardTokenSnapshot?> ProgressSnapshot { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<AgentTaskForAgentDto?> FetchTaskAsync(
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null) =>
            throw new NotSupportedException();

        public Task PatchTaskAsync(
            Guid taskId,
            PatchAgentTaskDto patch,
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null) =>
            throw new NotSupportedException();

        public Task ReportProgressAsync(
            Guid taskId,
            AgentTaskProgressDto progress,
            CancellationToken ct,
            DashboardTokenSnapshot? tokenSnapshot = null)
        {
            ProgressSnapshot.TrySetResult(tokenSnapshot);
            return Task.CompletedTask;
        }
    }
}
