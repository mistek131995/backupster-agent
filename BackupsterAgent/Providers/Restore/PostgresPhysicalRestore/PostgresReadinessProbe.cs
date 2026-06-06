using BackupsterAgent.Configuration;
using BackupsterAgent.Services.Common.Resolvers;
using Npgsql;

namespace BackupsterAgent.Providers.Restore.PostgresPhysicalRestore;

public sealed class PostgresReadinessProbe : IPostgresReadinessProbe
{
    private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromSeconds(2);

    private readonly ILogger<PostgresReadinessProbe> _logger;
    private readonly Func<ConnectionConfig, CancellationToken, Task> _probeConnectionAsync;
    private readonly TimeSpan _retryDelay;

    public PostgresReadinessProbe(ILogger<PostgresReadinessProbe> logger)
        : this(logger, ProbeConnectionAsync, DefaultRetryDelay)
    {
    }

    internal PostgresReadinessProbe(
        ILogger<PostgresReadinessProbe> logger,
        Func<ConnectionConfig, CancellationToken, Task> probeConnectionAsync,
        TimeSpan retryDelay)
    {
        _logger = logger;
        _probeConnectionAsync = probeConnectionAsync;
        _retryDelay = retryDelay < TimeSpan.Zero ? TimeSpan.Zero : retryDelay;
    }

    public async Task WaitUntilReadyAsync(ConnectionConfig connection, TimeSpan timeout, CancellationToken ct)
    {
        timeout = timeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(1) : timeout;
        var deadline = DateTime.UtcNow.Add(timeout);
        Exception? lastFailure = null;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                await _probeConnectionAsync(connection, ct);
                _logger.LogInformation(
                    "PostgreSQL connection '{ConnectionName}' is accepting SQL connections on {Host}:{Port}",
                    connection.Name, connection.Host, connection.Port);
                return;
            }
            catch (PostgresException ex) when (IsTransientStartupError(ex))
            {
                lastFailure = ex;
            }
            catch (PostgresException ex)
            {
                _logger.LogWarning(ex,
                    "PostgreSQL connection '{ConnectionName}' is reachable but rejected the readiness probe - treating server as ready",
                    connection.Name);
                return;
            }
            catch (NpgsqlException ex)
            {
                lastFailure = ex;
            }
            catch (TimeoutException ex)
            {
                lastFailure = ex;
            }

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                throw BuildTimeoutException(connection, timeout, lastFailure);

            var delay = _retryDelay < remaining ? _retryDelay : remaining;
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, ct);
        }
    }

    private static async Task ProbeConnectionAsync(ConnectionConfig connection, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(PostgresConnectionFactory.BuildAdminConnectionString(connection));
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand("SELECT 1;", conn);
        await cmd.ExecuteScalarAsync(ct);
    }

    private static bool IsTransientStartupError(PostgresException ex) =>
        ex.SqlState is "57P01" or "57P02" or "57P03";

    private static InvalidOperationException BuildTimeoutException(
        ConnectionConfig connection,
        TimeSpan timeout,
        Exception? lastFailure)
    {
        var seconds = Math.Ceiling(timeout.TotalSeconds);
        var message =
            $"PostgreSQL '{connection.Name}' не начал принимать подключения в течение {seconds:0} секунд после запуска. " +
            "Проверьте log PostgreSQL и состояние восстановленного PGDATA.";

        if (lastFailure is not null)
            message += $" Последняя ошибка: {lastFailure.Message}";

        return new InvalidOperationException(message, lastFailure);
    }
}
