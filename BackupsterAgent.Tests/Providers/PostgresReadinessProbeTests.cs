using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace BackupsterAgent.Tests.Providers;

public sealed class PostgresReadinessProbeTests
{
    [Test]
    public async Task WaitUntilReadyAsync_TransientFailureThenSuccess_Retries()
    {
        var calls = 0;
        var probe = new PostgresReadinessProbe(
            NullLogger<PostgresReadinessProbe>.Instance,
            (_, _) =>
            {
                calls++;
                if (calls == 1)
                    throw new TimeoutException("connection refused");

                return Task.CompletedTask;
            },
            TimeSpan.FromMilliseconds(1));

        await probe.WaitUntilReadyAsync(Connection(), TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public async Task WaitUntilReadyAsync_PostgresAuthFailure_TreatsServerAsReady()
    {
        var calls = 0;
        var probe = new PostgresReadinessProbe(
            NullLogger<PostgresReadinessProbe>.Instance,
            (_, _) =>
            {
                calls++;
                throw new PostgresException(
                    messageText: "password authentication failed",
                    severity: "FATAL",
                    invariantSeverity: "FATAL",
                    sqlState: "28P01");
            },
            TimeSpan.FromMilliseconds(1));

        await probe.WaitUntilReadyAsync(Connection(), TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void WaitUntilReadyAsync_Timeout_ThrowsClearError()
    {
        var probe = new PostgresReadinessProbe(
            NullLogger<PostgresReadinessProbe>.Instance,
            (_, _) => throw new TimeoutException("still starting"),
            TimeSpan.FromMilliseconds(1));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() =>
            probe.WaitUntilReadyAsync(Connection(), TimeSpan.FromMilliseconds(25), CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("не начал принимать подключения"));
        Assert.That(ex.InnerException, Is.TypeOf<TimeoutException>());
    }

    [Test]
    public void WaitUntilReadyAsync_Canceled_PropagatesCancellation()
    {
        var probe = new PostgresReadinessProbe(
            NullLogger<PostgresReadinessProbe>.Instance,
            (_, ct) => Task.Delay(TimeSpan.FromSeconds(10), ct),
            TimeSpan.FromMilliseconds(1));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() =>
            probe.WaitUntilReadyAsync(Connection(), TimeSpan.FromSeconds(1), cts.Token));
    }

    private static ConnectionConfig Connection() => new()
    {
        Name = "pg-main",
        DatabaseType = DatabaseType.Postgres,
        Host = "localhost",
        Port = 5432,
        Username = "postgres",
        Password = string.Empty,
    };
}
