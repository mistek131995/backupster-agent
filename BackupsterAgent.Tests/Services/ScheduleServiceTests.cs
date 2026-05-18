using System.Text.Json;
using BackupsterAgent.Configuration;
using BackupsterAgent.Contracts;
using BackupsterAgent.Enums;
using BackupsterAgent.Services.Common.State;
using BackupsterAgent.Services.Dashboard;
using BackupsterAgent.Services.Dashboard.Clients;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class ScheduleServiceTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), $"schedule-svc-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch { }
    }

    [Test]
    public async Task NoCache_ReturnsEmpty()
    {
        var svc = CreateService(schedule: null);

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task DefaultActive_NoOverrides_ReturnsEmpty()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = "0 2 * * *",
            IsActive = true,
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Is.Empty,
            "default should be ignored by agent now — only per-(db,mode) overrides are honored");
    }

    [Test]
    public async Task LogicalOverrideActive_ReturnsLogicalOnly()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].Mode, Is.EqualTo(BackupMode.Logical));
        Assert.That(entries[0].NextRun, Is.EqualTo(NextOccurrence("0 3 * * *")));
    }

    [Test]
    public async Task PhysicalOverrideActive_ReturnsPhysicalOnly()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].Mode, Is.EqualTo(BackupMode.Physical));
        Assert.That(entries[0].NextRun, Is.EqualTo(NextOccurrence("0 4 * * *")));
    }

    [Test]
    public async Task BothOverridesActive_ReturnsTwoIndependentEntries()
    {
        var logicalRun = NextOccurrence("0 3 * * *");
        var physicalRun = NextOccurrence("0 4 * * *");

        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(2));
        var byMode = entries.ToDictionary(e => e.Mode, e => e.NextRun);
        Assert.Multiple(() =>
        {
            Assert.That(byMode[BackupMode.Logical], Is.EqualTo(logicalRun));
            Assert.That(byMode[BackupMode.Physical], Is.EqualTo(physicalRun));
        });
    }

    [Test]
    public async Task InactiveOverride_Skipped()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = false,
                    BackupMode = BackupMode.Logical,
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].Mode, Is.EqualTo(BackupMode.Physical));
    }

    [Test]
    public async Task AllOverridesInactive_ReturnsEmpty()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = "0 2 * * *",
            IsActive = true,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = false,
                    BackupMode = BackupMode.Logical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task OverrideForOtherDatabase_DoesNotLeak()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "orders",
                    CronExpression = "0 5 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task StorageNames_ThreeStorages_ReturnsThreeEntries_SameModeDifferentStorages()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main", "sftp-archive", "local-nas"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(3));
        var byStorage = entries.ToDictionary(e => e.StorageName!, e => e);
        Assert.Multiple(() =>
        {
            Assert.That(byStorage.Keys, Is.EquivalentTo(new[] { "s3-main", "sftp-archive", "local-nas" }));
            Assert.That(byStorage.Values.All(e => e.Mode == BackupMode.Logical), Is.True);
            Assert.That(byStorage.Values.Select(e => e.NextRun).Distinct().Count(), Is.EqualTo(1),
                "all three entries share the same cron, so NextRun must be identical");
        });
    }

    [Test]
    public async Task StorageNames_NullOrMissing_ReturnsOneEntryWithNullStorage()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    // StorageNames not set → null
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].StorageName, Is.Null,
            "null StorageNames must produce a single legacy-fallback entry with StorageName = null");
    }

    [Test]
    public async Task StorageNames_EmptyArray_ReturnsOneEntryWithNullStorage()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = [],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].StorageName, Is.Null);
    }

    [Test]
    public async Task StorageNames_WhitespaceElements_AreSkipped()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main", "", "  ", "sftp-archive"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(2));
        Assert.That(entries.Select(e => e.StorageName), Is.EquivalentTo(new[] { "s3-main", "sftp-archive" }));
    }

    [Test]
    public async Task StorageNames_MixedOverridesAcrossModes_AreExpandedIndependently()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main", "sftp-archive"],
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                    StorageNames = ["local-nas"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(3));
        var logical = entries.Where(e => e.Mode == BackupMode.Logical).ToList();
        var physical = entries.Where(e => e.Mode == BackupMode.Physical).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(logical.Select(e => e.StorageName),
                Is.EquivalentTo(new[] { "s3-main", "sftp-archive" }));
            Assert.That(physical, Has.Count.EqualTo(1));
            Assert.That(physical[0].StorageName, Is.EqualTo("local-nas"));
        });
    }

    [Test]
    public async Task TwoDatabasesIndependent_EachQueriedSeparately()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "orders",
                    CronExpression = "0 5 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                },
            ],
        });

        var payments = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);
        var orders = await svc.GetDueSchedulesAsync("orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(payments, Has.Count.EqualTo(2));
            Assert.That(orders, Has.Count.EqualTo(1));
            Assert.That(orders[0].Mode, Is.EqualTo(BackupMode.Logical));
            Assert.That(orders[0].NextRun, Is.EqualTo(NextOccurrence("0 5 * * *")));
        });
    }

    [Test]
    public async Task Override_WithExplicitId_PropagatesIntoScheduleEntry()
    {
        var explicitId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    Id = explicitId,
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main", "sftp-archive"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(2));
        Assert.That(entries.Select(e => e.ScheduleId).Distinct(), Is.EquivalentTo(new[] { explicitId }),
            "explicit Id must be reused for every storage entry derived from the same override");
    }

    [Test]
    public async Task Override_WithoutId_GetsDeterministicFakeId()
    {
        ScheduleDto Build() => new()
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main"],
                },
            ],
        };

        var first = await CreateService(Build()).GetDueSchedulesAsync("payments", CancellationToken.None);
        var second = await CreateService(Build()).GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(first[0].ScheduleId, Is.Not.EqualTo(Guid.Empty));
        Assert.That(second[0].ScheduleId, Is.EqualTo(first[0].ScheduleId),
            "fake id must be deterministic across invocations so the run-tracker key stays stable between ticks");
    }

    [Test]
    public async Task Override_WithoutId_FakeId_DiffersByStorage()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main", "sftp-archive", "local-nas"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries.Select(e => e.ScheduleId).Distinct().Count(), Is.EqualTo(3),
            "fake ids must differ across storages so per-storage last-run is tracked independently");
    }

    [Test]
    public async Task Override_WithoutId_FakeId_DiffersByMode()
    {
        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 3 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main"],
                },
                new ScheduleOverrideDto
                {
                    DatabaseName = "payments",
                    CronExpression = "0 4 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Physical,
                    StorageNames = ["s3-main"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(2));
        Assert.That(entries.Select(e => e.ScheduleId).Distinct().Count(), Is.EqualTo(2),
            "same (db, storage) but different mode must produce different fake ids");
    }

    [Test]
    public async Task TwoOverridesSameModeAndStorage_DifferentExplicitIds_GetIndependentEntries()
    {
        var idMorning = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var idEvening = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

        var svc = CreateService(new ScheduleDto
        {
            CronExpression = string.Empty,
            IsActive = false,
            Overrides =
            [
                new ScheduleOverrideDto
                {
                    Id = idMorning,
                    Name = "morning",
                    DatabaseName = "payments",
                    CronExpression = "0 6 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main"],
                },
                new ScheduleOverrideDto
                {
                    Id = idEvening,
                    Name = "evening",
                    DatabaseName = "payments",
                    CronExpression = "0 20 * * *",
                    IsActive = true,
                    BackupMode = BackupMode.Logical,
                    StorageNames = ["s3-main"],
                },
            ],
        });

        var entries = await svc.GetDueSchedulesAsync("payments", CancellationToken.None);

        Assert.That(entries, Has.Count.EqualTo(2));
        Assert.That(entries.Select(e => e.ScheduleId), Is.EquivalentTo(new[] { idMorning, idEvening }),
            "two schedules with the same (db, mode, storage) but different ids must yield independent entries");
    }

    private ScheduleService CreateService(ScheduleDto? schedule)
    {
        var scheduleFile = Path.Combine(_root, "schedule.json");
        if (schedule is not null)
        {
            File.WriteAllText(scheduleFile,
                JsonSerializer.Serialize(schedule, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
        }

        var store = new ScheduleStore(scheduleFile, NullLogger<ScheduleStore>.Instance);
        var http = new HttpClient();
        var settings = Options.Create(new AgentSettings { Token = string.Empty, DashboardUrl = string.Empty });
        var authGuard = new FakeAuthGuard();

        return new ScheduleService(http, store, settings, authGuard, NullLogger<ScheduleService>.Instance);
    }

    private static DateTime NextOccurrence(string cron) =>
        ScheduleService.ComputeNextOccurrenceUtc(cron, DateTime.Now);

    [Test]
    public void ComputeNextOccurrenceUtc_InterpretsCronAsLocalTime()
    {
        var nowLocal = DateTime.SpecifyKind(new DateTime(2026, 5, 18, 8, 5, 0), DateTimeKind.Local);

        var resultUtc = ScheduleService.ComputeNextOccurrenceUtc("10 8 * * *", nowLocal);

        Assert.Multiple(() =>
        {
            Assert.That(resultUtc.Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That(resultUtc.ToLocalTime(),
                Is.EqualTo(DateTime.SpecifyKind(new DateTime(2026, 5, 18, 8, 10, 0), DateTimeKind.Local)));
        });
    }

    [Test]
    public void ComputeNextOccurrenceUtc_NextRunInFuture_ReturnsThatPoint()
    {
        var nowLocal = DateTime.SpecifyKind(new DateTime(2026, 5, 18, 8, 15, 0), DateTimeKind.Local);

        var resultUtc = ScheduleService.ComputeNextOccurrenceUtc("10 8 * * *", nowLocal);

        Assert.That(resultUtc.ToLocalTime(),
            Is.EqualTo(DateTime.SpecifyKind(new DateTime(2026, 5, 19, 8, 10, 0), DateTimeKind.Local)));
    }

    private sealed class FakeAuthGuard : IDashboardAuthGuard
    {
        public void OnUnauthorized(string channel, ILogger logger) { }
    }
}
