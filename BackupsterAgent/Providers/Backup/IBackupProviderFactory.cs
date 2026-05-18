using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Backup;

public interface IBackupProviderFactory
{
    IBackupProvider GetProvider(DatabaseType databaseType, BackupMode backupMode);

    IDifferentialBackupProvider GetDifferentialProvider(DatabaseType databaseType);
}
