using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed record MysqlDatadirSwapContext(
    ConnectionConfig Connection,
    string RealDatadir,
    string StagingPath,
    string OldPath,
    string FailedPath,
    MysqlInstanceInfo InstanceInfo);
