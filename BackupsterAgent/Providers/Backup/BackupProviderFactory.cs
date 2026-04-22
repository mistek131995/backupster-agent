using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Backup;

public sealed class BackupProviderFactory : IBackupProviderFactory
{
    private readonly PostgresLogicalBackupProvider _postgresLogical;
    private readonly MssqlPhysicalBackupProvider _mssqlPhysical;
    private readonly MssqlLogicalBackupProvider _mssqlLogical;
    private readonly MysqlLogicalBackupProvider _mysqlLogical;

    public BackupProviderFactory(
        PostgresLogicalBackupProvider postgresLogical,
        MssqlPhysicalBackupProvider mssqlPhysical,
        MssqlLogicalBackupProvider mssqlLogical,
        MysqlLogicalBackupProvider mysqlLogical)
    {
        _postgresLogical = postgresLogical;
        _mssqlPhysical = mssqlPhysical;
        _mssqlLogical = mssqlLogical;
        _mysqlLogical = mysqlLogical;
    }

    public IBackupProvider GetProvider(DatabaseType databaseType, BackupMode backupMode) =>
        (databaseType, backupMode) switch
        {
            (DatabaseType.Postgres, BackupMode.Logical)  => _postgresLogical,
            (DatabaseType.Mysql,    BackupMode.Logical)  => _mysqlLogical,
            (DatabaseType.Mssql,    BackupMode.Physical) => _mssqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Logical)  => _mssqlLogical,
            _ => throw new NotSupportedException(
                $"Backup provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };
}
