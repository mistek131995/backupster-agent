namespace DbBackupAgent.Enums;

public enum RestoreStage
{
    DownloadingDump,
    DecryptingDump,
    DecompressingDump,
    PreparingDatabase,
    RestoringDatabase,
    DownloadingManifest,
    RestoringFiles,
}
