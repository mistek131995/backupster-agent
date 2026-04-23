using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Backup;

public sealed class BackupProviderFactory : IBackupProviderFactory
{
    private readonly PostgresLogicalBackupProvider _postgresLogical;
    private readonly PostgresPhysicalBackupProvider _postgresPhysical;
    private readonly MssqlPhysicalBackupProvider _mssqlPhysical;
    private readonly MssqlLogicalBackupProvider _mssqlLogical;
    private readonly MysqlLogicalBackupProvider _mysqlLogical;
    private readonly MysqlPhysicalBackupProvider _mysqlPhysical;

    public BackupProviderFactory(
        PostgresLogicalBackupProvider postgresLogical,
        PostgresPhysicalBackupProvider postgresPhysical,
        MssqlPhysicalBackupProvider mssqlPhysical,
        MssqlLogicalBackupProvider mssqlLogical,
        MysqlLogicalBackupProvider mysqlLogical,
        MysqlPhysicalBackupProvider mysqlPhysical)
    {
        _postgresLogical = postgresLogical;
        _postgresPhysical = postgresPhysical;
        _mssqlPhysical = mssqlPhysical;
        _mssqlLogical = mssqlLogical;
        _mysqlLogical = mysqlLogical;
        _mysqlPhysical = mysqlPhysical;
    }

    public IBackupProvider GetProvider(DatabaseType databaseType, BackupMode backupMode) =>
        (databaseType, backupMode) switch
        {
            (DatabaseType.Postgres, BackupMode.Logical)  => _postgresLogical,
            (DatabaseType.Postgres, BackupMode.Physical) => _postgresPhysical,
            (DatabaseType.Mysql,    BackupMode.Logical)  => _mysqlLogical,
            (DatabaseType.Mysql,    BackupMode.Physical) => _mysqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Physical) => _mssqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Logical)  => _mssqlLogical,
            _ => throw new NotSupportedException(
                $"Backup provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };
}
