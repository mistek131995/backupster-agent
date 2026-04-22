using BackupsterAgent.Configuration;
using BackupsterAgent.Domain;

namespace BackupsterAgent.Providers.Backup;

public interface IBackupProvider
{
    Task<BackupResult> BackupAsync(DatabaseConfig config, ConnectionConfig connection, CancellationToken ct);
}
