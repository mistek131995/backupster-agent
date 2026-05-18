using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Providers.Restore;

public interface IDifferentialRestoreProvider
{
    Task ValidatePermissionsAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct);

    Task ValidateRestoreSourceAsync(
        ConnectionConfig connection,
        IReadOnlyList<DifferentialRestoreChainItem> chain,
        CancellationToken ct);

    Task PrepareTargetDatabaseAsync(ConnectionConfig connection, string targetDatabase, CancellationToken ct);

    Task RestoreAsync(
        ConnectionConfig connection,
        string targetDatabase,
        IReadOnlyList<DifferentialRestoreChainItem> chain,
        CancellationToken ct);
}
