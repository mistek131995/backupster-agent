using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using BackupsterAgent.Configuration;
using Polly;
using Polly.Retry;

namespace BackupsterAgent.Services.Dashboard;

public abstract class DashboardClientBase
{
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(4),
    ];

    protected static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        };

    protected readonly AgentSettings Settings;
    protected readonly IDashboardAuthGuard AuthGuard;

    protected DashboardClientBase(AgentSettings settings, IDashboardAuthGuard authGuard)
    {
        Settings = NormalizeUrl(settings);
        AuthGuard = authGuard;
    }

    private static AgentSettings NormalizeUrl(AgentSettings settings)
    {
        var url = settings.DashboardUrl;
        if (!string.IsNullOrWhiteSpace(url) &&
            !url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
            !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            url = "https://" + url;
        }

        return url == settings.DashboardUrl
            ? settings
            : new AgentSettings { Token = settings.Token, DashboardUrl = url };
    }

    protected bool IsConfigured(ILogger logger, string clientName)
    {
        if (!string.IsNullOrWhiteSpace(Settings.Token) && !string.IsNullOrWhiteSpace(Settings.DashboardUrl))
            return true;

        logger.LogWarning(
            "{Client}: AgentSettings.Token or DashboardUrl is not configured.", clientName);
        return false;
    }

    protected void ThrowIfUnauthorized(HttpResponseMessage response, string channel, ILogger logger)
    {
        if (response.StatusCode != HttpStatusCode.Unauthorized) return;
        AuthGuard.OnUnauthorized(channel, logger);
        throw new DashboardUnauthorizedException(channel);
    }

    protected ResiliencePipeline BuildRetryPipeline(string clientName, ILogger logger) =>
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                ShouldHandle = args => ValueTask.FromResult(
                    args.Outcome.Exception is not null and not DashboardUnauthorizedException),
                DelayGenerator = args =>
                {
                    var delay = args.AttemptNumber < RetryDelays.Length
                        ? RetryDelays[args.AttemptNumber]
                        : RetryDelays[^1];
                    return ValueTask.FromResult<TimeSpan?>(delay);
                },
                OnRetry = args =>
                {
                    logger.LogWarning(
                        "{Client}: retry {Attempt}/3. Error: {Message}",
                        clientName, args.AttemptNumber + 1, args.Outcome.Exception?.Message);
                    return ValueTask.CompletedTask;
                },
            })
            .Build();
}
