using System.Net;

namespace BackupsterAgent.Providers.Secrets;

internal sealed class VaultApiException : HttpRequestException
{
    public VaultApiException(string message, HttpStatusCode statusCode, IReadOnlyList<string> errors)
        : base(message, null, statusCode)
    {
        Errors = errors;
    }

    public IReadOnlyList<string> Errors { get; }

    public bool ContainsError(string marker) =>
        Errors.Any(error => error.Contains(marker, StringComparison.OrdinalIgnoreCase));
}
