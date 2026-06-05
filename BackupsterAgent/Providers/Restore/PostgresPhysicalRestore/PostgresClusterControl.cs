namespace BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

internal enum PostgresClusterControlKind
{
    Unmanaged,
    Systemd,
    WindowsService,
}

internal sealed record PostgresClusterControl(
    PostgresClusterControlKind Kind,
    string? ServiceName,
    string? ServiceAccount,
    int? PostmasterPid);
