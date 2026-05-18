using BackupsterAgent.Enums;

namespace BackupsterAgent.Services.Dashboard.Clients;

public readonly record struct ScheduleEntry(Guid ScheduleId, BackupMode Mode, DateTime NextRun, string? StorageName);
