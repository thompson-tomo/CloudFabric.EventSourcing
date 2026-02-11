using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.Projections;

public interface IProjectionsEngine : IAsyncDisposable
{
    Task StartAsync(string instanceName);

    Task StopAsync();

    void AddProjectionBuilder(IProjectionBuilder projectionBuilder);

    Task RebuildOneAsync(Guid documentId, string partitionKey);

    Task ReplayEventsAsync(
        string instanceName,
        string? partitionKey,
        DateTime? dateFrom,
        int chunkSize = 250,
        Func<int, IEvent, Task>? chunkProcessedCallback = null,
        CancellationToken cancellationToken = default
    );

    Task<EventStoreStatistics> GetEventStoreStatistics();
}
