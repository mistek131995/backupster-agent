using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Restore;

public sealed class RestoreProviderFactory : IRestoreProviderFactory
{
    private readonly PostgresRestoreProvider _postgresLogical;
    private readonly PostgresPhysicalRestoreProvider _postgresPhysical;
    private readonly MssqlPhysicalRestoreProvider _mssqlPhysical;
    private readonly MssqlLogicalRestoreProvider _mssqlLogical;
    private readonly MysqlRestoreProvider _mysql;
    private readonly MysqlPhysicalRestoreProvider _mysqlPhysical;

    public RestoreProviderFactory(
        PostgresRestoreProvider postgresLogical,
        PostgresPhysicalRestoreProvider postgresPhysical,
        MssqlPhysicalRestoreProvider mssqlPhysical,
        MssqlLogicalRestoreProvider mssqlLogical,
        MysqlRestoreProvider mysql,
        MysqlPhysicalRestoreProvider mysqlPhysical)
    {
        _postgresLogical = postgresLogical;
        _postgresPhysical = postgresPhysical;
        _mssqlPhysical = mssqlPhysical;
        _mssqlLogical = mssqlLogical;
        _mysql = mysql;
        _mysqlPhysical = mysqlPhysical;
    }

    public IRestoreProvider GetProvider(DatabaseType databaseType, BackupMode backupMode) =>
        (databaseType, backupMode) switch
        {
            (DatabaseType.Postgres, BackupMode.Logical)  => _postgresLogical,
            (DatabaseType.Postgres, BackupMode.Physical) => _postgresPhysical,
            (DatabaseType.Mysql,    BackupMode.Logical)  => _mysql,
            (DatabaseType.Mysql,    BackupMode.Physical) => _mysqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Physical) => _mssqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Logical)  => _mssqlLogical,
            _ => throw new NotSupportedException(
                $"Restore provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };
}
