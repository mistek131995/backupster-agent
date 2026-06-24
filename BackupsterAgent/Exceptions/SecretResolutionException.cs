namespace BackupsterAgent.Exceptions;

public sealed class SecretResolutionException : Exception
{
    public SecretResolutionException(string message) : base(message) { }

    public SecretResolutionException(string message, Exception innerException) : base(message, innerException) { }
}
