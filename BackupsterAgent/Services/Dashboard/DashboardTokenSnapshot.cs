namespace BackupsterAgent.Services.Dashboard;

public sealed class DashboardTokenSnapshot
{
    private string? _token;

    internal DashboardTokenSnapshot(string? token)
    {
        _token = token;
    }

    internal string? Token => Volatile.Read(ref _token);

    internal bool IsAvailable => !string.IsNullOrWhiteSpace(Token);

    internal string? ReplaceIfCurrent(string expectedToken, string replacementToken)
    {
        var observed = Interlocked.CompareExchange(ref _token, replacementToken, expectedToken);
        return string.Equals(observed, expectedToken, StringComparison.Ordinal)
            ? replacementToken
            : observed;
    }
}
