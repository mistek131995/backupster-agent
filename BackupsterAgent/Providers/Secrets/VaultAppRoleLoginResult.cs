namespace BackupsterAgent.Providers.Secrets;

public sealed record VaultAppRoleLoginResult(
    string ClientToken,
    int LeaseDurationSeconds,
    bool Renewable);
