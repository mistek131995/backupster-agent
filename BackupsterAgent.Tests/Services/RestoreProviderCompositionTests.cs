using BackupsterAgent.Configuration;
using BackupsterAgent.Extensions;
using BackupsterAgent.Providers.Restore;
using BackupsterAgent.Providers.Restore.Common;
using BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;
using BackupsterAgent.Services.Common.Processes;
using BackupsterAgent.Services.Common.Resolvers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BackupsterAgent.Tests.Services;

public sealed class RestoreProviderCompositionTests
{
    [Test]
    public void ServiceProvider_ResolvesPhysicalRestoreProviders()
    {
        var services = new ServiceCollection();

        services.AddLogging();
        services.AddSingleton<IExternalProcessRunner, StubProcessRunner>();
        services.AddSingleton(Options.Create(new RestoreSettings()));
        services.AddSingleton<PostgresBinaryResolver>();
        services.AddSingleton<MysqlBinaryResolver>();
        services.AddSingleton<RestorePathResolver>();
        services.AddSingleton<RestoreMarkerStore>();
        services.AddSingleton<FilesystemRenamePreflight>();
        services.AddSingleton<LinuxProcessInspector>();
        services.AddSingleton<SystemdUnitDetector>();
        services.AddSingleton<SystemdServiceController>();
        services.AddSingleton<PostgresClusterLifecycle>();
        services.AddSingleton<IPostgresReadinessProbe, PostgresReadinessProbe>();
        services.AddSingleton<PostgresPhysicalRestoreProvider>();
        services.AddMysqlPhysicalRestore();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });

        Assert.DoesNotThrow(() => provider.GetRequiredService<MysqlPhysicalRestoreProvider>());
        Assert.DoesNotThrow(() => provider.GetRequiredService<PostgresPhysicalRestoreProvider>());
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
