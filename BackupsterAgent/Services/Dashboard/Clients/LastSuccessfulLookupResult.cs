using BackupsterAgent.Contracts;

namespace BackupsterAgent.Services.Dashboard.Clients;

public enum LastSuccessfulLookupOutcome
{
    Found,
    NotFound,
    DashboardUnavailable,
}

public sealed record LastSuccessfulLookupResult(
    LastSuccessfulLookupOutcome Outcome,
    LastSuccessfulBackupResponseDto? Body = null);
