using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using BackupsterAgent.Configuration;
using BackupsterAgent.Exceptions;
using BackupsterAgent.Services.Common.Secrets;
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
    protected readonly ISecretResolver Secrets;

    protected DashboardClientBase(
        AgentSettings settings,
        IDashboardAuthGuard authGuard,
        ISecretResolver secrets)
    {
        Settings = NormalizeUrl(settings);
        AuthGuard = authGuard;
        Secrets = secrets;
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
            : new AgentSettings
            {
                Token = settings.Token,
                TokenSecret = settings.TokenSecret,
                DashboardUrl = url,
            };
    }

    protected async Task<string?> ResolveTokenOrSkipAsync(
        ILogger logger,
        string clientName,
        CancellationToken ct,
        DashboardTokenSnapshot? tokenSnapshot = null)
    {
        if (string.IsNullOrWhiteSpace(Settings.DashboardUrl))
        {
            logger.LogWarning(
                "{Client}: AgentSettings.DashboardUrl is not configured.", clientName);
            return null;
        }

        if (tokenSnapshot is not null)
            return tokenSnapshot.Token;

        string token;
        try
        {
            token = await Secrets.ResolveStringAsync(
                Settings.TokenSecret,
                Settings.Token,
                "AgentSettings.Token",
                ct);
        }
        catch (SecretResolutionException ex)
        {
            logger.LogError(ex, "{Client}: failed to resolve AgentSettings.Token.", clientName);
            return null;
        }

        if (!string.IsNullOrWhiteSpace(token))
            return token;

        logger.LogWarning(
            "{Client}: AgentSettings.Token is not configured.", clientName);
        return null;
    }

    protected void ThrowIfUnauthorized(HttpResponseMessage response, string channel, ILogger logger)
    {
        if (response.StatusCode != HttpStatusCode.Unauthorized) return;
        AuthGuard.OnUnauthorized(channel, logger);
        throw new DashboardUnauthorizedException(channel);
    }

    protected async Task<HttpResponseMessage> SendWithTokenRefreshAsync(
        HttpClient http,
        Func<string, HttpRequestMessage> requestFactory,
        string token,
        DashboardTokenSnapshot? tokenSnapshot,
        string channel,
        ILogger logger,
        CancellationToken ct)
    {
        token = tokenSnapshot?.Token ?? token;
        var response = await SendAsync(http, requestFactory, token, ct);
        if (response.StatusCode != HttpStatusCode.Unauthorized)
            return response;

        var refreshedToken = await TryRefreshTokenSnapshotAsync(
            tokenSnapshot, token, channel, logger, ct);
        if (string.IsNullOrWhiteSpace(refreshedToken))
        {
            response.Dispose();
            AuthGuard.OnUnauthorized(channel, logger);
            throw new DashboardUnauthorizedException(channel);
        }

        response.Dispose();
        response = await SendAsync(http, requestFactory, refreshedToken, ct);
        if (response.StatusCode != HttpStatusCode.Unauthorized)
            return response;

        response.Dispose();
        AuthGuard.OnUnauthorized(channel, logger);
        throw new DashboardUnauthorizedException(channel);
    }

    private async Task<string?> TryRefreshTokenSnapshotAsync(
        DashboardTokenSnapshot? tokenSnapshot,
        string rejectedToken,
        string channel,
        ILogger logger,
        CancellationToken ct)
    {
        if (tokenSnapshot is null)
            return null;

        var currentToken = tokenSnapshot.Token;
        if (!string.Equals(currentToken, rejectedToken, StringComparison.Ordinal))
            return currentToken;

        string refreshedToken;
        try
        {
            refreshedToken = await Secrets.ResolveStringAsync(
                Settings.TokenSecret,
                Settings.Token,
                "AgentSettings.Token",
                ct);
        }
        catch (SecretResolutionException ex)
        {
            logger.LogError(ex, "{Channel}: failed to refresh AgentSettings.Token after HTTP 401.", channel);
            return null;
        }

        if (string.IsNullOrWhiteSpace(refreshedToken) ||
            string.Equals(refreshedToken, rejectedToken, StringComparison.Ordinal))
        {
            return null;
        }

        var effectiveToken = tokenSnapshot.ReplaceIfCurrent(rejectedToken, refreshedToken);
        logger.LogInformation("{Channel}: dashboard token snapshot refreshed after HTTP 401.", channel);
        return effectiveToken;
    }

    private static async Task<HttpResponseMessage> SendAsync(
        HttpClient http,
        Func<string, HttpRequestMessage> requestFactory,
        string token,
        CancellationToken ct)
    {
        using var request = requestFactory(token);
        return await http.SendAsync(request, ct);
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
