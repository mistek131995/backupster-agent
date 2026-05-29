using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Backup;

public sealed class BackupProviderFactory : IBackupProviderFactory
{
    private readonly PostgresLogicalBackupProvider _postgresLogical;
    private readonly PostgresPhysicalBackupProvider _postgresPhysical;
    private readonly PostgresPhysicalDifferentialBackupProvider _postgresPhysicalDiff;
    private readonly MssqlPhysicalBackupProvider _mssqlPhysical;
    private readonly MssqlPhysicalDifferentialBackupProvider _mssqlPhysicalDiff;
    private readonly MssqlLogicalBackupProvider _mssqlLogical;
    private readonly MysqlLogicalBackupProvider _mysqlLogical;
    private readonly MysqlPhysicalBackupProvider _mysqlPhysical;
    private readonly MongoLogicalBackupProvider _mongoLogical;

    public BackupProviderFactory(
        PostgresLogicalBackupProvider postgresLogical,
        PostgresPhysicalBackupProvider postgresPhysical,
        PostgresPhysicalDifferentialBackupProvider postgresPhysicalDiff,
        MssqlPhysicalBackupProvider mssqlPhysical,
        MssqlPhysicalDifferentialBackupProvider mssqlPhysicalDiff,
        MssqlLogicalBackupProvider mssqlLogical,
        MysqlLogicalBackupProvider mysqlLogical,
        MysqlPhysicalBackupProvider mysqlPhysical,
        MongoLogicalBackupProvider mongoLogical)
    {
        _postgresLogical = postgresLogical;
        _postgresPhysical = postgresPhysical;
        _postgresPhysicalDiff = postgresPhysicalDiff;
        _mssqlPhysical = mssqlPhysical;
        _mssqlPhysicalDiff = mssqlPhysicalDiff;
        _mssqlLogical = mssqlLogical;
        _mysqlLogical = mysqlLogical;
        _mysqlPhysical = mysqlPhysical;
        _mongoLogical = mongoLogical;
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
            (DatabaseType.MongoDb,  BackupMode.Logical)  => _mongoLogical,
            _ => throw new NotSupportedException(
                $"Backup provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };

    public IDifferentialBackupProvider GetDifferentialProvider(DatabaseType databaseType) =>
        databaseType switch
        {
            DatabaseType.Postgres => _postgresPhysicalDiff,
            DatabaseType.Mssql    => _mssqlPhysicalDiff,
            _ => throw new NotSupportedException(
                $"Differential backup is not supported for DatabaseType='{databaseType}'.")
        };
}
