using DbBackupAgent.Enums;

namespace DbBackupAgent.Services.Common;

public interface IProgressReporterFactory
{
    IProgressReporter<RestoreStage> CreateForRestore(Guid taskId);

    IProgressReporter<BackupStage> CreateForBackup(Guid backupRecordId);
}
