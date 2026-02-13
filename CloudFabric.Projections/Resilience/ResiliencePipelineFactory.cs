using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace CloudFabric.Projections.Resilience;

public static class ResiliencePipelineFactory
{
    /// <summary>
    /// Builds a <see cref="ResiliencePipeline"/> with exponential-backoff retry and optional circuit breaker.
    /// Each backend supplies its own <paramref name="shouldHandle"/> predicate to decide which exceptions are transient.
    /// </summary>
    public static ResiliencePipeline Create(
        ResilienceSettings settings,
        PredicateBuilder<object> shouldHandle,
        ILogger logger,
        string operationName = "Database")
    {
        var builder = new ResiliencePipelineBuilder();

        builder.AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = shouldHandle,
            MaxRetryAttempts = settings.MaxRetryCount,
            BackoffType = DelayBackoffType.Exponential,
            Delay = settings.BaseDelay,
            MaxDelay = settings.MaxDelay,
            UseJitter = true,
            OnRetry = args =>
            {
                logger.LogWarning(
                    args.Outcome.Exception,
                    "{OperationName} transient failure, retry {RetryAttempt}/{MaxRetries} after {Delay}ms",
                    operationName,
                    args.AttemptNumber + 1,
                    settings.MaxRetryCount,
                    args.RetryDelay.TotalMilliseconds
                );
                return default;
            }
        });

        if (settings.CircuitBreakerEnabled)
        {
            builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                ShouldHandle = shouldHandle,
                FailureRatio = 0.5,
                MinimumThroughput = settings.CircuitBreakerFailureThreshold,
                BreakDuration = settings.CircuitBreakerBreakDuration,
                SamplingDuration = settings.CircuitBreakerSamplingDuration,
                OnOpened = args =>
                {
                    logger.LogError(
                        "{OperationName} circuit breaker opened for {BreakDuration}s due to repeated failures",
                        operationName,
                        args.BreakDuration.TotalSeconds
                    );
                    return default;
                },
                OnClosed = _ =>
                {
                    logger.LogInformation("{OperationName} circuit breaker closed, resuming normal operation", operationName);
                    return default;
                }
            });
        }

        return builder.Build();
    }
}
