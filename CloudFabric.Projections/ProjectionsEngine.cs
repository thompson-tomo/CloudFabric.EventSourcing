using System.Collections.Immutable;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections.Queries;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CloudFabric.Projections;

public class ProjectionsEngine : IProjectionsEngine
{
    private ImmutableList<IProjectionBuilder<ProjectionDocument>> _projectionBuilders = ImmutableList<IProjectionBuilder<ProjectionDocument>>.Empty;
    private ImmutableList<IProjectionBuilder> _dynamicProjectionBuilders = ImmutableList<IProjectionBuilder>.Empty;

    private EventsObserver? _observer;
    private readonly ILogger<ProjectionsEngine> _logger;

    public ProjectionsEngine(ILogger<ProjectionsEngine> logger)
    {
        _logger = logger;
    }

    public ProjectionsEngine() : this(NullLogger<ProjectionsEngine>.Instance)
    {
    }

    public Task StartAsync(string instanceName)
    {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before StartAsync");
        }

        return _observer.StartAsync(instanceName);
    }

    /// <summary>
    /// Synchronous version of StartAsync for observers whose StartAsync completes synchronously
    /// (e.g. InMemory, PostgreSQL). Throws if the observer's StartAsync does not complete immediately.
    /// </summary>
    public void Start(string instanceName)
    {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before Start");
        }

        var task = _observer.StartAsync(instanceName);
        if (!task.IsCompletedSuccessfully)
        {
            throw new InvalidOperationException(
                "Observer's StartAsync did not complete synchronously. " +
                "Use StartAsync() for observers with asynchronous initialization (e.g. CosmosDb change feed).");
        }
    }

    public Task StopAsync()
    {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before StopAsync");
        }

        return _observer.StopAsync();
    }

    public void SetEventsObserver(EventsObserver eventsObserver)
    {
        _observer = eventsObserver;
        _observer.SetEventHandler(HandleEvent);
    }

    public void AddProjectionBuilder(IProjectionBuilder<ProjectionDocument> projectionBuilder)
    {
        ImmutableInterlocked.Update(ref _projectionBuilders, list => list.Add(projectionBuilder));
    }

    public void AddProjectionBuilder(IProjectionBuilder projectionBuilder)
    {
        ImmutableInterlocked.Update(ref _dynamicProjectionBuilders, list => list.Add(projectionBuilder));
    }

    public async Task RebuildOneAsync(Guid documentId, string partitionKey)
    {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before RebuildAsync");
        }

        await _observer.ReplayEventsForOneDocumentAsync(documentId, partitionKey);
    }

    private async Task HandleEvent(IEvent @event)
    {
        var eventType = @event.GetType();

        foreach (var projectionBuilder in _projectionBuilders)
        {
            if (!projectionBuilder.HandledEventTypes.Contains(eventType))
            {
                continue;
            }

            try
            {
                await projectionBuilder.ApplyEvent(@event);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Projection builder {BuilderType} failed to handle event {EventType} for aggregate {AggregateId}",
                    projectionBuilder.GetType().Name, eventType.Name, @event.AggregateId);
            }
        }

        foreach (var projectionBuilder in _dynamicProjectionBuilders)
        {
            if (!projectionBuilder.HandledEventTypes.Contains(eventType))
            {
                continue;
            }

            try
            {
                await projectionBuilder.ApplyEvent(@event);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dynamic projection builder {BuilderType} failed to handle event {EventType} for aggregate {AggregateId}",
                    projectionBuilder.GetType().Name, eventType.Name, @event.AggregateId);
            }
        }

        #region Apply AggregateUpdatedEvent to projection builders

        var aggregateType = Type.GetType(@event.AggregateType);

        if (aggregateType == null)
        {
            _logger.LogWarning("Failed to get type of aggregate {AggregateType}, skipping AggregateUpdatedEvent dispatch", @event.AggregateType);
            return;
        }

        var aggregateUpdatedEventType = typeof(AggregateUpdatedEvent<>).MakeGenericType(aggregateType);

        var buildersWithAggregateUpdatedEvent = _projectionBuilders.Where(
            p => !p.HandledEventTypes.Contains(eventType) && p.HandledEventTypes.Contains(aggregateUpdatedEventType)
        ).ToList();

        var dynamicBuildersWithAggregateUpdatedEvent = _dynamicProjectionBuilders.Where(
            p => !p.HandledEventTypes.Contains(eventType) && p.HandledEventTypes.Contains(aggregateUpdatedEventType)
        ).ToList();

        if (buildersWithAggregateUpdatedEvent.Count > 0 || dynamicBuildersWithAggregateUpdatedEvent.Count > 0)
        {
            var aggregateUpdatedEvent = (IEvent)Activator.CreateInstance(aggregateUpdatedEventType)!;
            aggregateUpdatedEvent.AggregateId = @event.AggregateId;
            aggregateUpdatedEvent.PartitionKey = @event.PartitionKey;
            aggregateUpdatedEvent.AggregateType = @event.AggregateType;
            aggregateUpdatedEventType.GetProperty(nameof(AggregateUpdatedEvent<object>.UpdatedAt))!.SetValue(aggregateUpdatedEvent, @event.Timestamp);

            foreach (var projectionBuilder in buildersWithAggregateUpdatedEvent)
            {
                try
                {
                    await projectionBuilder.ApplyEvent(aggregateUpdatedEvent);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Projection builder {BuilderType} failed to handle AggregateUpdatedEvent for aggregate {AggregateId}",
                        projectionBuilder.GetType().Name, @event.AggregateId);
                }
            }

            foreach (var projectionBuilder in dynamicBuildersWithAggregateUpdatedEvent)
            {
                try
                {
                    await projectionBuilder.ApplyEvent(aggregateUpdatedEvent);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Dynamic projection builder {BuilderType} failed to handle AggregateUpdatedEvent for aggregate {AggregateId}",
                        projectionBuilder.GetType().Name, @event.AggregateId);
                }
            }
        }

        #endregion
    }

    public async Task ReplayEventsAsync(
        string instanceName,
        string? partitionKey,
        DateTime? dateFrom,
        int chunkSize = 250,
        Func<int, IEvent, Task>? chunkProcessedCallback = null,
        CancellationToken cancellationToken = default
    ) {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before ReplayEventsAsync");
        }

        await _observer.ReplayEventsAsync(
            instanceName,
            partitionKey,
            dateFrom,
            chunkSize,
            chunkProcessedCallback,
            cancellationToken
        );
    }

    public async Task<EventStoreStatistics> GetEventStoreStatistics()
    {
        if (_observer == null)
        {
            throw new InvalidOperationException("SetEventsObserver should be called before GetEventStoreStatistics");
        }

        return await _observer.GetEventStoreStatistics();
    }
}
