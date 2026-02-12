namespace CloudFabric.EventSourcing.EventStore;

/// <summary>
/// Marker interface for events that logically affect multiple aggregates at once.
/// These events are stored in a dedicated global stream (keyed by target aggregate type)
/// and are NOT part of any individual aggregate's event stream in the event store.
///
/// When loading an aggregate, global events are merged into the aggregate's event stream
/// by timestamp, allowing the aggregate to apply them via its On(TEvent) handler.
///
/// In projections, these events are handled via IHandleCrossAggregateEvent&lt;T&gt; and use
/// bulk UpdateByQuery operations instead of per-document updates.
/// </summary>
public interface ICrossAggregateEvent : IEvent
{
    /// <summary>
    /// Partition key of the target aggregates. Required for tenant isolation.
    /// </summary>
    string TargetPartitionKey { get; set; }
}
