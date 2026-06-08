using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace BackupsterAgent.Tests.Services;

[TestFixture]
public sealed class PostgresQueryRetryTests
{
    [Test]
    public async Task ExecuteAsync_TransientPostgresStartupError_Retries()
    {
        var calls = 0;

        var result = await PostgresQueryRetry.ExecuteAsync(
            NullLogger.Instance,
            "SHOW data_directory",
            "pg-main",
            _ =>
            {
                calls++;
                if (calls == 1)
                    throw new PostgresException(
                        messageText: "terminating connection due to administrator command",
                        severity: "FATAL",
                        invariantSeverity: "FATAL",
                        sqlState: "57P01");

                return Task.FromResult("ready");
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo("ready"));
            Assert.That(calls, Is.EqualTo(2));
        });
    }

    [Test]
    public void ExecuteAsync_NonTransientPostgresError_DoesNotRetry()
    {
        var calls = 0;

        var ex = Assert.ThrowsAsync<PostgresException>(() =>
            PostgresQueryRetry.ExecuteAsync<string>(
                NullLogger.Instance,
                "SHOW data_directory",
                "pg-main",
                _ =>
                {
                    calls++;
                    throw new PostgresException(
                        messageText: "permission denied",
                        severity: "ERROR",
                        invariantSeverity: "ERROR",
                        sqlState: "42501");
                },
                CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SqlState, Is.EqualTo("42501"));
            Assert.That(calls, Is.EqualTo(1));
        });
    }
}
