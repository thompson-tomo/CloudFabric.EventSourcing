namespace CloudFabric.Projections.Resilience;

public class ResilienceSettings
{
    /// <summary>
    /// Maximum number of retry attempts for transient failures.
    /// </summary>
    public int MaxRetryCount { get; set; } = 5;

    /// <summary>
    /// Base delay for exponential backoff. Actual delay = BaseDelay * 2^attempt + jitter.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Maximum delay cap for a single retry wait.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether to enable circuit breaker on top of retries.
    /// When enabled, repeated failures will cause the circuit to open and fast-fail requests.
    /// </summary>
    public bool CircuitBreakerEnabled { get; set; }

    /// <summary>
    /// Minimum number of failures within <see cref="CircuitBreakerSamplingDuration"/> before the circuit opens.
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    /// <summary>
    /// How long the circuit stays open before allowing a probe request.
    /// </summary>
    public TimeSpan CircuitBreakerBreakDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Sampling duration for failure rate calculation.
    /// </summary>
    public TimeSpan CircuitBreakerSamplingDuration { get; set; } = TimeSpan.FromSeconds(60);

    public static ResilienceSettings Default => new();
}
