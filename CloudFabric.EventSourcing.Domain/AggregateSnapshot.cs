namespace CloudFabric.EventSourcing.Domain;

/// <summary>
/// Snapshot of an aggregate's state at a specific version.
/// Enables loading aggregates by restoring a snapshot and replaying only new events,
/// instead of replaying all events from the beginning.
/// </summary>
public class AggregateSnapshot
{
    public Guid StreamId { get; set; }

    public string PartitionKey { get; set; } = "";

    /// <summary>Assembly-qualified type name of the aggregate, used to reconstruct the correct derived type.</summary>
    public string AggregateType { get; set; } = "";

    /// <summary>The aggregate's own-event version at the time the snapshot was created.</summary>
    public int Version { get; set; }

    /// <summary>JSON-serialized aggregate state.</summary>
    public string StateJson { get; set; } = "";

    /// <summary>
    /// Timestamp of the last event (own or cross-aggregate) that was applied when this snapshot was created.
    /// Used to filter cross-aggregate (global) events during restoration: only events with
    /// Timestamp strictly greater than this value need to be replayed.
    /// </summary>
    public DateTime LastAppliedEventTimestamp { get; set; }
}
