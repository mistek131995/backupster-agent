using BackupsterAgent.Enums;

namespace BackupsterAgent.Configuration;

public sealed class ConnectionConfig
{
    public string Name { get; init; } = string.Empty;
    public DatabaseType DatabaseType { get; init; } = DatabaseType.Postgres;
    public string? ConnectionUri { get; init; }
    public SecretRef? ConnectionUriSecret { get; init; }
    public string Host { get; init; } = string.Empty;
    public int Port { get; init; } = 5432;
    public string Username { get; init; } = string.Empty;
    public SecretRef? UsernameSecret { get; init; }
    public string Password { get; init; } = string.Empty;
    public SecretRef? PasswordSecret { get; init; }
    public string? BinPath { get; init; }
}
