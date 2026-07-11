using BackupsterAgent.Enums;
using BackupsterAgent.Services.Dashboard;

namespace BackupsterAgent.Services.Common.Progress;

public interface IProgressReporterFactory
{
    IProgressReporter<RestoreStage> CreateForRestore(
        Guid taskId,
        DashboardTokenSnapshot tokenSnapshot);

    IProgressReporter<DeleteStage> CreateForDelete(
        Guid taskId,
        DashboardTokenSnapshot tokenSnapshot);

    IProgressReporter<BackupStage> CreateForBackup(
        Guid backupRecordId,
        DashboardTokenSnapshot tokenSnapshot,
        bool offline = false);
}
