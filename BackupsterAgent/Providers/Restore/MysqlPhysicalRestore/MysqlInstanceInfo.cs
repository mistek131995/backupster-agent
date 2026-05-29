namespace BackupsterAgent.Providers.Restore.MysqlPhysicalRestore;

public sealed record MysqlInstanceInfo(
    IReadOnlyList<string> OriginalArgs,
    int? Pid,
    string? OwnerUser,
    string? OwnerGroup,
    string? ServiceName);
