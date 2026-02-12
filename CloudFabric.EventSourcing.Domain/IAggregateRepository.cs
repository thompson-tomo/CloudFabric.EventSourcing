using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;

namespace CloudFabric.EventSourcing.Domain;

/// <summary>
/// Repository for working with domain entities
/// </summary>
/// <typeparam name="T">Domain entity derived from AggregateBase</typeparam>
public interface IAggregateRepository<T> where T : AggregateBase
{
    Task<T?> LoadAsync(Guid id, string partitionKey, CancellationToken cancellationToken = default);

    Task<T> LoadAsyncOrThrowNotFound(Guid id, string partitionKey, CancellationToken cancellationToken = default);

    Task<bool> SaveAsync(EventUserInfo eventUserInfo, T aggregate, CancellationToken cancellationToken = default);

    Task<bool> HardDeleteAsync(Guid id, string partitionKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Batch-saves multiple NEW aggregates in a single event store transaction.
    /// No version check — all aggregates are assumed to be new (version 0).
    /// Event handlers are notified after commit for projection building.
    /// </summary>
    Task SaveMultipleNewAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<T> aggregates,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Batch-appends events to multiple EXISTING aggregate streams without loading aggregates.
    /// For bulk updates where events contain absolute values (e.g., SetPrice, SetCategory).
    /// AggregateType is set automatically from T. Stream versions are loaded automatically.
    /// Event handlers are notified after commit for projection building.
    /// </summary>
    Task AppendEventsToMultipleAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<(Guid StreamId, string PartitionKey, IReadOnlyList<IEvent> Events)> streams,
        CancellationToken cancellationToken = default
    );
}
