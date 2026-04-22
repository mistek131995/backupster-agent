namespace BackupsterAgent.Services.Dashboard.Clients;

public sealed record OpenRecordResult(DashboardAvailability Status, Guid? Id = null);
