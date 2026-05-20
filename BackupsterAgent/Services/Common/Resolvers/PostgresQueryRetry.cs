using System.Net.Sockets;
using Npgsql;

namespace BackupsterAgent.Services.Common.Resolvers;

public static class PostgresQueryRetry
{
    private static readonly TimeSpan[] DefaultDelays =
    {
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(3),
    };

    public static Task ExecuteAsync(
        ILogger logger,
        string operation,
        string connectionName,
        Func<CancellationToken, Task> action,
        CancellationToken ct)
        => ExecuteAsync<object?>(logger, operation, connectionName, async innerCt =>
        {
            await action(innerCt);
            return null;
        }, ct);

    public static async Task<T> ExecuteAsync<T>(
        ILogger logger,
        string operation,
        string connectionName,
        Func<CancellationToken, Task<T>> action,
        CancellationToken ct)
    {
        var attempt = 0;
        var maxAttempts = DefaultDelays.Length + 1;

        while (true)
        {
            attempt++;
            try
            {
                return await action(ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (PostgresException)
            {
                throw;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                if (attempt >= maxAttempts)
                {
                    logger.LogError(ex,
                        "Postgres operation '{Operation}' on connection '{Connection}' failed after {Attempts} attempts",
                        operation, connectionName, attempt);

                    throw new InvalidOperationException(
                        $"Соединение с PostgreSQL '{connectionName}' прервалось при выполнении \"{operation}\" " +
                        $"(попыток: {attempt}). Проверьте сеть и idle-таймауты на стороне сервера/pgBouncer/прокси. " +
                        $"Детали: {ex.Message}", ex);
                }

                var delay = DefaultDelays[attempt - 1];
                logger.LogWarning(ex,
                    "Postgres operation '{Operation}' on connection '{Connection}' failed (attempt {Attempt}/{Total}), retrying in {Delay}s",
                    operation, connectionName, attempt, maxAttempts, delay.TotalSeconds);

                await Task.Delay(delay, ct);
            }
        }
    }

    private static bool IsTransient(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is NpgsqlException && current is not PostgresException) return true;
            if (current is IOException) return true;
            if (current is SocketException) return true;
        }
        return false;
    }
}
