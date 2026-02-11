using System.Collections.Immutable;
using CloudFabric.EventSourcing.EventStore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CloudFabric.Projections;

public class ProjectionsEngine : IProjectionsEngine
{
    private ImmutableList<IProjectionBuilder> _projectionBuilders = ImmutableList<IProjectionBuilder>.Empty;

    private readonly EventsObserver _observer;
    private readonly ILogger<ProjectionsEngine> _logger;
    private readonly IProjectionErrorHandler? _errorHandler;

    public ProjectionsEngine(
        EventsObserver eventsObserver,
        ILogger<ProjectionsEngine> logger,
        IProjectionErrorHandler? errorHandler = null
    )
    {
        _observer = eventsObserver ?? throw new ArgumentNullException(nameof(eventsObserver));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _errorHandler = errorHandler;
        _observer.AddEventHandler(HandleEvent);
    }

    public ProjectionsEngine(EventsObserver eventsObserver) : this(eventsObserver, NullLogger<ProjectionsEngine>.Instance)
    {
    }

    public static ProjectionsEngineBuilder CreateBuilder() => new ProjectionsEngineBuilder();

    public Task StartAsync(string instanceName)
    {
        return _observer.StartAsync(instanceName);
    }

    public async Task StopAsync()
    {
        _observer.RemoveEventHandler(HandleEvent);
        await _observer.StopAsync();
    }

    public void AddProjectionBuilder(IProjectionBuilder projectionBuilder)
    {
        ImmutableInterlocked.Update(ref _projectionBuilders, list => list.Add(projectionBuilder));
    }

    public async Task RebuildOneAsync(Guid documentId, string partitionKey)
    {
        await _observer.ReplayEventsForOneDocumentAsync(HandleEvent, documentId, partitionKey);
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
                await HandleProjectionError(projectionBuilder, @event, ex);
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

        if (buildersWithAggregateUpdatedEvent.Count > 0)
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
                    await HandleProjectionError(projectionBuilder, aggregateUpdatedEvent, ex);
                }
            }
        }

        #endregion
    }

    private async Task HandleProjectionError(IProjectionBuilder projectionBuilder, IEvent @event, Exception ex)
    {
        _logger.LogError(ex, "Projection builder {BuilderType} failed to handle event {EventType} for aggregate {AggregateId}",
            projectionBuilder.GetType().Name, @event.GetType().Name, @event.AggregateId);

        if (_errorHandler != null)
        {
            await _errorHandler.OnError(projectionBuilder, @event, ex);
        }
    }

    public async Task ReplayEventsAsync(
        string instanceName,
        string? partitionKey,
        DateTime? dateFrom,
        int chunkSize = 250,
        Func<int, IEvent, Task>? chunkProcessedCallback = null,
        CancellationToken cancellationToken = default
    ) {
        await _observer.ReplayEventsAsync(
            HandleEvent,
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
        return await _observer.GetEventStoreStatistics();
    }

    public ValueTask DisposeAsync()
    {
        _observer.RemoveEventHandler(HandleEvent);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
