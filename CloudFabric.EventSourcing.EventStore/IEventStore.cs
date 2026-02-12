using CloudFabric.EventSourcing.EventStore.Persistence;

namespace CloudFabric.EventSourcing.EventStore;

public interface IEventStore : IAsyncDisposable
{
    Task<EventStream> LoadStreamAsyncOrThrowNotFound(Guid streamId, string partitionKey, CancellationToken cancellationToken =  default);

    Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default);

    Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, int fromVersion, CancellationToken cancellationToken = default);

    Task<LoadEventsResult> LoadEventsAsync(
        string? partitionKey,
        DateTime? dateFrom = null,
        int limit = 250,
        string? continuationToken = null,
        CancellationToken cancellationToken = default
    );
    
    Task<bool> AppendToStreamAsync(
        EventUserInfo eventUserInfo,
        Guid streamId,
        int expectedVersion,
        IEnumerable<IEvent> events,
        CancellationToken cancellationToken = default
    );

    Task Initialize(CancellationToken cancellationToken = default);

    Task<EventStoreStatistics> GetStatistics(CancellationToken cancellationToken = default);

    Task DeleteAll(CancellationToken cancellationToken = default);

    Task<bool> HardDeleteAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends a cross-aggregate event to the global stream. The stream_id is derived
    /// deterministically from the event's AggregateType (target aggregate type).
    /// No optimistic concurrency check is performed — global events are independent.
    /// </summary>
    Task<bool> AppendGlobalEventAsync(
        EventUserInfo eventUserInfo,
        ICrossAggregateEvent @event,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Loads all global (cross-aggregate) events for a given aggregate type.
    /// Includes events targeting the specified partition key and events targeting all partitions ("*").
    /// </summary>
    Task<List<IEvent>> LoadGlobalEventsAsync(
        string aggregateType,
        string? partitionKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Batch-insert events for multiple NEW streams in a single transaction.
    /// No version check is performed — all streams are assumed to be new (version 0).
    /// After commit, event handlers are notified for all events.
    /// </summary>
    /// <param name="eventUserInfo">User information attached to all events.</param>
    /// <param name="streams">List of (StreamId, PartitionKey, Events) tuples for new aggregates.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AppendNewStreamsAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<(Guid StreamId, string PartitionKey, IReadOnlyList<IEvent> Events)> streams,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Batch-append events to multiple EXISTING streams in a single transaction.
    /// Loads current stream versions automatically and assigns sequential version numbers.
    /// If a concurrent modification is detected (UNIQUE violation), the entire transaction fails.
    /// After commit, event handlers are notified for all events.
    /// </summary>
    /// <param name="eventUserInfo">User information attached to all events.</param>
    /// <param name="streams">List of (StreamId, PartitionKey, Events) tuples for existing aggregates.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AppendToMultipleExistingStreamsAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<(Guid StreamId, string PartitionKey, IReadOnlyList<IEvent> Events)> streams,
        CancellationToken cancellationToken = default
    );
}