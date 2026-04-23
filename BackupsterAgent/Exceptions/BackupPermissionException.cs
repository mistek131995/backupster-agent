namespace BackupsterAgent.Exceptions;

public sealed class BackupPermissionException : Exception
{
    public BackupPermissionException(string message) : base(message) { }
}
