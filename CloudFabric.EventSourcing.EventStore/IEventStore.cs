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
}