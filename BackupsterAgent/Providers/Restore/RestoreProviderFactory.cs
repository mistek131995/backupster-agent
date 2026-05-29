using BackupsterAgent.Enums;

namespace BackupsterAgent.Providers.Restore;

public sealed class RestoreProviderFactory : IRestoreProviderFactory
{
    private readonly PostgresRestoreProvider _postgresLogical;
    private readonly PostgresPhysicalRestoreProvider _postgresPhysical;
    private readonly PostgresPhysicalDifferentialRestoreProvider _postgresPhysicalDiff;
    private readonly MssqlPhysicalRestoreProvider _mssqlPhysical;
    private readonly MssqlPhysicalDifferentialRestoreProvider _mssqlPhysicalDiff;
    private readonly MssqlLogicalRestoreProvider _mssqlLogical;
    private readonly MysqlRestoreProvider _mysql;
    private readonly MysqlPhysicalRestoreProvider _mysqlPhysical;
    private readonly MongoRestoreProvider _mongo;

    public RestoreProviderFactory(
        PostgresRestoreProvider postgresLogical,
        PostgresPhysicalRestoreProvider postgresPhysical,
        PostgresPhysicalDifferentialRestoreProvider postgresPhysicalDiff,
        MssqlPhysicalRestoreProvider mssqlPhysical,
        MssqlPhysicalDifferentialRestoreProvider mssqlPhysicalDiff,
        MssqlLogicalRestoreProvider mssqlLogical,
        MysqlRestoreProvider mysql,
        MysqlPhysicalRestoreProvider mysqlPhysical,
        MongoRestoreProvider mongo)
    {
        _postgresLogical = postgresLogical;
        _postgresPhysical = postgresPhysical;
        _postgresPhysicalDiff = postgresPhysicalDiff;
        _mssqlPhysical = mssqlPhysical;
        _mssqlPhysicalDiff = mssqlPhysicalDiff;
        _mssqlLogical = mssqlLogical;
        _mysql = mysql;
        _mysqlPhysical = mysqlPhysical;
        _mongo = mongo;
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
            (DatabaseType.MongoDb,  BackupMode.Logical)  => _mongo,
            _ => throw new NotSupportedException(
                $"Restore provider is not implemented for DatabaseType='{databaseType}', BackupMode='{backupMode}'.")
        };

    public IDifferentialRestoreProvider GetDifferentialProvider(DatabaseType databaseType) =>
        databaseType switch
        {
            DatabaseType.Postgres => _postgresPhysicalDiff,
            DatabaseType.Mssql    => _mssqlPhysicalDiff,
            _ => throw new NotSupportedException(
                $"Differential restore is not supported for DatabaseType='{databaseType}'.")
        };
}
