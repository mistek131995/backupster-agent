namespace BackupsterAgent.Providers.Backup.MssqlPhysicalDifferentialBackup;

public enum MssqlDifferentialChainCheck
{
    Ok,
    ParentMissing,
    BaseUnknownOrAmbiguous,
    BaseDiverged,
    ForeignFullDetected,
}
