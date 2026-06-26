using BackupsterAgent.Configuration;

namespace BackupsterAgent.Providers.Secrets;

public interface ISecretProvider
{
    string EmptyValueSourceName { get; }
    bool SupportsSynchronousReads { get; }
    bool CanRead(string provider);
    Task<string> ReadAsync(SecretRef secret, string settingPath, CancellationToken ct);
    string Read(SecretRef secret, string settingPath);
}
