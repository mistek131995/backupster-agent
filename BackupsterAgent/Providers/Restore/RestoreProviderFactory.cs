using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Restore;

public sealed class RestoreProviderFactory : IRestoreProviderFactory
{
    private readonly PostgresRestoreProvider _postgres;
    private readonly MssqlPhysicalRestoreProvider _mssqlPhysical;
    private readonly MssqlLogicalRestoreProvider _mssqlLogical;
    private readonly MysqlRestoreProvider _mysql;

    public RestoreProviderFactory(
        PostgresRestoreProvider postgres,
        MssqlPhysicalRestoreProvider mssqlPhysical,
        MssqlLogicalRestoreProvider mssqlLogical,
        MysqlRestoreProvider mysql)
    {
        _postgres = postgres;
        _mssqlPhysical = mssqlPhysical;
        _mssqlLogical = mssqlLogical;
        _mysql = mysql;
    }

    public IRestoreProvider GetProvider(DatabaseType databaseType, BackupMode backupMode) =>
        (databaseType, backupMode) switch
        {
            (DatabaseType.Postgres, BackupMode.Logical)  => _postgres,
            (DatabaseType.Mysql,    BackupMode.Logical)  => _mysql,
            (DatabaseType.Mssql,    BackupMode.Physical) => _mssqlPhysical,
            (DatabaseType.Mssql,    BackupMode.Logical)  => _mssqlLogical,
            _ => throw new NotSupportedException(
                $"Restore provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };
}
