using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Restore;

public interface IRestoreProviderFactory
{
    IRestoreProvider GetProvider(DatabaseType databaseType, BackupMode backupMode);
}
