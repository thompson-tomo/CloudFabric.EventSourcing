namespace CloudFabric.EventSourcing.EventStore;

public class LoadEventsResult
{
    public List<IEvent> Events { get; init; } = new();

    /// <summary>
    /// Opaque token for cursor-based pagination. Pass this to the next LoadEventsAsync call
    /// to continue from where the previous call left off. Null when there are no more events.
    /// </summary>
    public string? ContinuationToken { get; init; }
}
