namespace CloudFabric.Projections;

public class BatchBufferOptions
{
    /// <summary>
    /// Maximum number of buffered operations (upserts + deletes) before auto-flush is triggered.
    /// Set to 0 to disable auto-flush (unbounded buffer, existing behavior).
    /// Default: 1000.
    /// </summary>
    public int MaxBufferSize { get; set; } = 1000;
}
