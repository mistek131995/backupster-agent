using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services;
using BackupsterAgent.Services.Common;
using BackupsterAgent.Services.Common.Progress;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Clients;

namespace BackupsterAgent.Tests;

internal static class TestHelpers
{
    public static IProgressReporter<T> NullReporter<T>() where T : struct, Enum =>
        new NullProgressReporter<T>();
}

internal sealed class FakeProgressReporterFactory : IProgressReporterFactory
{
    public DashboardTokenSnapshot? LastRestoreTokenSnapshot { get; private set; }
    public DashboardTokenSnapshot? LastDeleteTokenSnapshot { get; private set; }
    public DashboardTokenSnapshot? LastBackupTokenSnapshot { get; private set; }

    public IProgressReporter<RestoreStage> CreateForRestore(
        Guid taskId,
        DashboardTokenSnapshot tokenSnapshot)
    {
        LastRestoreTokenSnapshot = tokenSnapshot;
        return new NullProgressReporter<RestoreStage>();
    }

    public IProgressReporter<DeleteStage> CreateForDelete(
        Guid taskId,
        DashboardTokenSnapshot tokenSnapshot)
    {
        LastDeleteTokenSnapshot = tokenSnapshot;
        return new NullProgressReporter<DeleteStage>();
    }

    public IProgressReporter<BackupStage> CreateForBackup(
        Guid backupRecordId,
        DashboardTokenSnapshot tokenSnapshot,
        bool offline = false)
    {
        LastBackupTokenSnapshot = tokenSnapshot;
        return new NullProgressReporter<BackupStage>();
    }
}

internal sealed class FakeDashboardTokenSnapshotProvider : IDashboardTokenSnapshotProvider
{
    public DashboardTokenSnapshot Snapshot { get; } = new("test-dashboard-token");
    public int CaptureCalls { get; private set; }

    public Task<DashboardTokenSnapshot> CaptureAsync(CancellationToken ct)
    {
        CaptureCalls++;
        return Task.FromResult(Snapshot);
    }
}

internal sealed class FakeBackupRecordClient : IBackupRecordClient
{
    public OpenRecordResult NextOpen { get; set; } = new(DashboardAvailability.Ok, Guid.NewGuid());
    public FinalizeRecordResult NextFinalize { get; set; } = new(DashboardAvailability.Ok);

    public int OpenCalls { get; private set; }
    public int ProgressCalls { get; private set; }
    public int FinalizeCalls { get; private set; }
    public OpenBackupRecordDto? LastOpen { get; private set; }
    public FinalizeBackupRecordDto? LastFinalize { get; private set; }
    public DashboardTokenSnapshot? LastOpenTokenSnapshot { get; private set; }
    public DashboardTokenSnapshot? LastProgressTokenSnapshot { get; private set; }
    public DashboardTokenSnapshot? LastFinalizeTokenSnapshot { get; private set; }

    public List<OpenBackupRecordDto> AllOpens { get; } = new();
    public List<FinalizeBackupRecordDto> AllFinalizes { get; } = new();

    public Task<OpenRecordResult> OpenAsync(
        OpenBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        OpenCalls++;
        LastOpen = dto;
        LastOpenTokenSnapshot = tokenSnapshot;
        AllOpens.Add(dto);
        return Task.FromResult(NextOpen);
    }

    public Task ReportProgressAsync(
        Guid backupRecordId,
        BackupProgressDto progress,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        ProgressCalls++;
        LastProgressTokenSnapshot = tokenSnapshot;
        return Task.CompletedTask;
    }

    public Task<FinalizeRecordResult> FinalizeAsync(
        Guid backupRecordId,
        FinalizeBackupRecordDto dto,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        FinalizeCalls++;
        LastFinalize = dto;
        LastFinalizeTokenSnapshot = tokenSnapshot;
        AllFinalizes.Add(dto);
        return Task.FromResult(NextFinalize);
    }

    public LastSuccessfulLookupResult NextLastSuccessful { get; set; } =
        new LastSuccessfulLookupResult(LastSuccessfulLookupOutcome.NotFound);
    public int LastSuccessfulCalls { get; private set; }

    public Task<LastSuccessfulLookupResult> GetLastSuccessfulAsync(
        string database,
        string storage,
        BackupMode mode,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        LastSuccessfulCalls++;
        return Task.FromResult(NextLastSuccessful);
    }
}
