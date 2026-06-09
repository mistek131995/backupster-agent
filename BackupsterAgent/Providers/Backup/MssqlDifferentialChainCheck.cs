namespace BackupsterAgent.Providers.Backup;

public enum MssqlDifferentialChainCheck
{
    Ok,
    ParentMissing,
    BaseUnknownOrAmbiguous,
    BaseDiverged,
    ForeignFullDetected,
}
