using System.Collections.Immutable;
using CloudFabric.EventSourcing.EventStore;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections;

public abstract class EventsObserver
{
    private ImmutableList<Func<IEvent, Task>> _eventHandlers = ImmutableList<Func<IEvent, Task>>.Empty;
    protected readonly ILogger<EventsObserver> _logger;
    protected readonly IEventStore _eventStore;

    protected EventsObserver(IEventStore eventStore, ILogger<EventsObserver> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    public void AddEventHandler(Func<IEvent, Task> eventHandler)
    {
        ImmutableInterlocked.Update(ref _eventHandlers, list => list.Add(eventHandler));
    }

    public void RemoveEventHandler(Func<IEvent, Task> eventHandler)
    {
        ImmutableInterlocked.Update(ref _eventHandlers, list => list.Remove(eventHandler));
    }

    public abstract Task StartAsync(string instanceName);

    public abstract Task StopAsync();

    public virtual async Task ReplayEventsForOneDocumentAsync(Func<IEvent, Task> eventHandler, Guid documentId, string partitionKey)
    {
        var stream = await _eventStore.LoadStreamAsync(documentId, partitionKey);

        foreach (var @event in stream.Events)
        {
            await eventHandler(@event);
        }
    }

    public virtual Task<EventStoreStatistics> GetEventStoreStatistics()
    {
        return _eventStore.GetStatistics();
    }

    /// <summary>
    /// Reads all events and runs event handlers on them (basically, "replays" those events). Needed for projections (materialized views)
    /// rebuild.
    /// </summary>
    /// <param name="instanceName">Used for tracking purposes. You can pass machineName or processId here.</param>
    /// <param name="partitionKey">PartitionKey to filter all events by.</param>
    /// <param name="dateFrom">Skip events which happened prior to this date.</param>
    /// <param name="chunkSize">How many events to load at a time.</param>
    /// <param name="chunkProcessedCallback">Function that will be called after each chunk of `chunkSize` is processed. Arguments: number of processed events (can be lower than chunkSize), last processed event</param>
    /// <param name="cancellationToken">This is a long-running operation, so make sure to pass correct CancellationToken here.</param>
    /// <returns></returns>
    public virtual async Task ReplayEventsAsync(
        Func<IEvent, Task> eventHandler,
        string instanceName,
        string? partitionKey,
        DateTime? dateFrom,
        int chunkSize = 250,
        Func<int, IEvent, Task>? chunkProcessedCallback = null,
        CancellationToken cancellationToken = default
    ) {
        _logger.LogInformation("Replaying events on {InstanceName} starting from timestamp: {DateFrom}",
            instanceName,
            dateFrom
        );

        string? continuationToken = null;
        var totalEventsProcessed = 0;
        var totalTime = TimeSpan.Zero;

        while (true)
        {
            var loadEventsWatch = System.Diagnostics.Stopwatch.StartNew();

            var result = await _eventStore.LoadEventsAsync(
                partitionKey,
                dateFrom,
                chunkSize,
                continuationToken,
                cancellationToken
            );

            loadEventsWatch.Stop();

            if (result.Events.Count <= 0)
            {
                _logger.LogInformation(
                    "Finished replaying events on {InstanceName} starting from timestamp: {DateFrom}, total events processed: {TotalEventsProcessed}, " +
                    "time took: {TotalTimeTook}",
                    instanceName, dateFrom, totalEventsProcessed, totalTime
                );

                break;
            }

            var applyEventsWatch = System.Diagnostics.Stopwatch.StartNew();

            foreach (var @event in result.Events)
            {
                await eventHandler(@event);
            }

            applyEventsWatch.Stop();

            continuationToken = result.ContinuationToken;
            var lastEvent = result.Events.Last();
            totalEventsProcessed += result.Events.Count;
            totalTime = totalTime.Add(loadEventsWatch.Elapsed).Add(applyEventsWatch.Elapsed);

            _logger.LogInformation(
                "Replayed chunk of {ReplayedEventsCount} on {InstanceName}, " +
                "reading chunk took {ReadingEventsMs}ms, applying events took {ApplyEventsMs}ms, " +
                "last event timestamp: {LastEventDateTime}",
                result.Events.Count, instanceName,
                loadEventsWatch.ElapsedMilliseconds, applyEventsWatch.ElapsedMilliseconds,
                lastEvent.Timestamp
            );

            if (chunkProcessedCallback != null)
            {
                await chunkProcessedCallback(result.Events.Count, lastEvent);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Cancellation requested. Processed {TotalEventsProcessed} {InstanceName}",
                    totalEventsProcessed,
                    instanceName
                );

                break;
            }
        }
    }

    protected async Task EventStoreOnEventAdded(IEvent e)
    {
        var handlers = Volatile.Read(ref _eventHandlers);

        if (handlers.IsEmpty)
        {
            throw new InvalidOperationException(
                "Can't process an event: no event handlers registered. Please call AddEventHandler before calling StartAsync.");
        }

        foreach (var handler in handlers)
        {
            await handler(e);
        }
    }
}
