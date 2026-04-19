namespace DbBackupAgent.Enums;

public enum BackupStage
{
    Dumping,
    EncryptingDump,
    UploadingDump,
    CapturingFiles,
}
