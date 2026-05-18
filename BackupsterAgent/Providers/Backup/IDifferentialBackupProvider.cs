using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Providers.Backup;

public interface IDifferentialBackupProvider
{
    Task ValidatePermissionsAsync(ConnectionConfig connection, string database, CancellationToken ct);

    Task<BackupResult> BackupAsync(
        DatabaseConfig config,
        ConnectionConfig connection,
        DifferentialBackupContext context,
        CancellationToken ct);
}
