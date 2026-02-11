namespace CloudFabric.EventSourcing.EventStore;

/// <summary>
/// Base record for cross-aggregate events. Stores in a global stream keyed by target aggregate type.
///
/// Storage convention:
/// - stream_id = DeterministicGuid(AggregateType) — isolates by target entity type
/// - partition_key = TargetPartitionKey ?? "*" — isolates by tenant ("*" = all tenants)
/// - AggregateId is set to Guid.Empty as a marker for global stream membership
/// </summary>
public record CrossAggregateEvent : Event, ICrossAggregateEvent
{
    /// <summary>
    /// Partition key wildcard value meaning "all partitions/tenants".
    /// </summary>
    public const string AllPartitionsKey = "*";

    public string? TargetPartitionKey { get; set; }

    public CrossAggregateEvent()
    {
        AggregateId = Guid.Empty;
    }
}
