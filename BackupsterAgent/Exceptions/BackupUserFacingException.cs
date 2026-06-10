namespace BackupsterAgent.Exceptions;

public sealed class BackupUserFacingException : Exception
{
    public BackupUserFacingException(string message) : base(message) { }

    public BackupUserFacingException(string message, Exception innerException) : base(message, innerException) { }
}
