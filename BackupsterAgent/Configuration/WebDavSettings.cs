namespace BackupsterAgent.Configuration;

public sealed class WebDavSettings
{
    public string BaseUrl { get; init; } = string.Empty;
    public string Username { get; init; } = string.Empty;
    public SecretRef? UsernameSecret { get; init; }
    public string Password { get; init; } = string.Empty;
    public SecretRef? PasswordSecret { get; init; }
    public string RemotePath { get; init; } = "/";
}
