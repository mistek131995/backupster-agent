namespace BackupsterAgent.Exceptions;

public sealed class DifferentialChainBrokenException : Exception
{
    public DifferentialChainBrokenException(string message) : base(message) { }
}
