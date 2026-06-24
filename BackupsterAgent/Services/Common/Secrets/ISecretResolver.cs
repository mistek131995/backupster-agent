using BackupsterAgent.Configuration;

namespace BackupsterAgent.Services.Common.Secrets;

public interface ISecretResolver
{
    Task<string> ResolveStringAsync(SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct);
    Task<string?> ResolveOptionalStringAsync(SecretRef? secret, string? plainValue, string settingPath, CancellationToken ct);
    string ResolveString(SecretRef? secret, string? plainValue, string settingPath);
    string? ResolveOptionalString(SecretRef? secret, string? plainValue, string settingPath);
    Task<ConnectionConfig> ResolveConnectionAsync(ConnectionConfig connection, CancellationToken ct);
    Task<StorageConfig> ResolveStorageAsync(StorageConfig storage, CancellationToken ct);
}
