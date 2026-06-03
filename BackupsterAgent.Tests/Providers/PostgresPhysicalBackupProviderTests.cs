using BackupsterAgent.Configuration;
using BackupsterAgent.Enums;
using BackupsterAgent.Providers.Backup;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackupsterAgent.Tests.Providers;

public sealed class PostgresPhysicalBackupProviderTests
{
    [Test]
    public void WarnIfInsufficientWalSendersAsync_CancelledToken_PropagatesCancellation()
    {
        var provider = new PostgresPhysicalBackupProvider(
            NullLogger<PostgresPhysicalBackupProvider>.Instance,
            new PostgresBinaryResolver(NullLogger<PostgresBinaryResolver>.Instance),
            new StubProcessRunner());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var connection = new ConnectionConfig
        {
            Name = "pg-cancelled",
            DatabaseType = DatabaseType.Postgres,
            Host = "localhost",
            Port = 5432,
            Username = "postgres",
            Password = string.Empty,
        };

        Assert.ThrowsAsync<OperationCanceledException>(
            () => provider.WarnIfInsufficientWalSendersAsync(connection, cts.Token));
    }

    private sealed class StubProcessRunner : IExternalProcessRunner
    {
        public Task<ExternalProcessResult> RunAsync(
            ExternalProcessRequest request,
            Func<Stream, CancellationToken, Task>? handleStdout,
            Func<StreamWriter, CancellationToken, Task>? handleStdin,
            CancellationToken ct) =>
            Task.FromResult(new ExternalProcessResult
            {
                ExitCode = 0,
                Stdout = string.Empty,
                Stderr = string.Empty,
            });
    }
}
