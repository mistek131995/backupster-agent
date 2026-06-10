namespace BackupsterAgent.Exceptions;

public sealed class RestorePermissionException : Exception
{
    public RestorePermissionException(string message) : base(message) { }

    public RestorePermissionException(string message, Exception innerException) : base(message, innerException) { }
}
