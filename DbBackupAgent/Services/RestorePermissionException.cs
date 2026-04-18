namespace DbBackupAgent.Services;

public sealed class RestorePermissionException : Exception
{
    public RestorePermissionException(string message) : base(message) { }
}
