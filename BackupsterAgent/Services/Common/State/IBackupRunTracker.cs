namespace BackupsterAgent.Services.Common.State;

public interface IBackupRunTracker
{
    void RecordRun(string databaseName, DateTime whenUtc);

    DateTime? GetLastRun(string databaseName);

    static string FileSetKey(string name) => $"fileset:{name}";
}
