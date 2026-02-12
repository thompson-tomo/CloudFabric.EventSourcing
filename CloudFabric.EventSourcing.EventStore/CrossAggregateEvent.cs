namespace CloudFabric.EventSourcing.EventStore;

/// <summary>
/// Base record for cross-aggregate events. Stores in a global stream keyed by target aggregate type.
///
/// Storage convention:
/// - stream_id = DeterministicGuid(AggregateType) — isolates by target entity type
/// - partition_key = TargetPartitionKey — isolates by tenant (required)
/// - AggregateId is set to Guid.Empty as a marker for global stream membership
/// </summary>
public record CrossAggregateEvent : Event, ICrossAggregateEvent
{
    public string TargetPartitionKey { get; set; } = string.Empty;

    public CrossAggregateEvent()
    {
        AggregateId = Guid.Empty;
    }
}
