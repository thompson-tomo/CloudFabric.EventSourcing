using System.Collections.ObjectModel;
using System.Net;
using System.Text.Json.Serialization;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.Projections;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.EventStore.CosmosDb;

public record Change : EventWrapper
{
    [JsonPropertyName("_lsn")] 
    public long LogicalSequenceNumber { get; set; }

    [JsonPropertyName("_ts")] 
    public long TimeStamp { get; set; }
}

public class CosmosDbEventStoreChangeFeedObserver : EventsObserver
{
    protected readonly CosmosClient _eventsClient;
    protected readonly string _eventsContainerId;
    protected readonly string _eventsDatabaseId;


    protected readonly CosmosClient _leaseClient;
    protected readonly string _leaseContainerId;
    protected readonly string _leaseDatabaseId;
    private ChangeFeedProcessor _changeFeedProcessor;

    private string _processorName;
    private readonly TimeSpan _changeFeedStartTimeOffset;

    public CosmosDbEventStoreChangeFeedObserver(
        CosmosClient eventsClient,
        string eventsDatabaseId,
        string eventsContainerId,
        CosmosClient leaseClient,
        string leaseDatabaseId,
        string leaseContainerId,
        string processorName,
        ILogger<CosmosDbEventStoreChangeFeedObserver> logger,
        TimeSpan? changeFeedStartTimeOffset = null
    ): base(new CosmosDbEventStore(eventsClient, eventsDatabaseId, eventsContainerId), logger)
    {
        _eventsClient = eventsClient;
        _eventsDatabaseId = eventsDatabaseId;
        _eventsContainerId = eventsContainerId;

        _leaseClient = leaseClient;
        _leaseDatabaseId = leaseDatabaseId;
        _leaseContainerId = leaseContainerId;

        _processorName = processorName;
        _changeFeedStartTimeOffset = changeFeedStartTimeOffset ?? TimeSpan.FromMinutes(-50);
    }

    public override Task StartAsync(string instanceName)
    {
        Container eventContainer = _eventsClient.GetContainer(_eventsDatabaseId, _eventsContainerId);
        Container leaseContainer = _leaseClient.GetContainer(_leaseDatabaseId, _leaseContainerId);

        // start with events at a specific time
        // https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed-processor
        //var myTime = DateTimeOffset.FromUnixTimeSeconds(_epochStartTime).UtcDateTime;

        _changeFeedProcessor = eventContainer
            .GetChangeFeedProcessorBuilder<Change>(_processorName, HandleChangesAsync)
            .WithInstanceName(instanceName)
            .WithLeaseContainer(leaseContainer)
            .WithStartTime(DateTime.UtcNow.Add(_changeFeedStartTimeOffset))
            .Build();
        
        _logger.LogInformation("Starting {InstanceName}", instanceName);

        return _changeFeedProcessor.StartAsync();
    }

    public override Task StopAsync()
    {
        _logger.LogInformation("Stopping");
        
        return _changeFeedProcessor.StopAsync();
    }

    public override async Task ReplayEventsAsync(
        string instanceName, 
        string? partitionKey, 
        DateTime? dateFrom,
        int chunkSize = 250,
        Func<int, IEvent, Task>? chunkProcessedCallback = null,
        CancellationToken cancellationToken = default
    )
    {
        _logger.LogInformation("Replaying events on {InstanceName} starting from timestamp: {DateFrom}",
            instanceName,
            dateFrom
        );
        
        Container eventContainer = _eventsClient.GetContainer(_eventsDatabaseId, _eventsContainerId);
        
        DateTime endTime = DateTime.UtcNow;

        using var feedIterator = eventContainer
            .GetChangeFeedIterator<Change>(
                dateFrom.HasValue 
                    ? ChangeFeedStartFrom.Time(dateFrom.Value, FeedRange.FromPartitionKey(new PartitionKey(partitionKey))) 
                    : ChangeFeedStartFrom.Beginning(FeedRange.FromPartitionKey(new PartitionKey(partitionKey))),
                ChangeFeedMode.Incremental,
                new ChangeFeedRequestOptions
                {
                    PageSizeHint = chunkSize
                }
            );

        while (feedIterator.HasMoreResults)
        {
            FeedResponse<Change> response = await feedIterator.ReadNextAsync(cancellationToken);

            if (response.All(x => x.GetEvent().Timestamp > endTime))
            {
                break;
            }

            var totalEventsProcessed = 0;
            
            if (response.StatusCode != HttpStatusCode.NotModified)
            {
                var events = new ReadOnlyCollection<Change>(response.ToList());
                totalEventsProcessed += events.Count;
                
                await HandleChangesAsync(events, CancellationToken.None);

                var lastEvent = events.Last().GetEvent();
                
                _logger.LogInformation("Replayed {ReplayedEventsCount} {InstanceName}, last event timestamp: {LastEventDateTime}", 
                    events.Count, 
                    instanceName,
                    lastEvent.Timestamp
                );

                if (chunkProcessedCallback != null)
                {
                    await chunkProcessedCallback(events.Count, lastEvent);
                }
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

    public override async Task<EventStoreStatistics> GetEventStoreStatistics()
    {
        return await _eventStore.GetStatistics();
    }

    public override async Task ReplayEventsForOneDocumentAsync(Guid documentId, string partitionKey)
    {
        Container eventContainer = _eventsClient.GetContainer(_eventsDatabaseId, _eventsContainerId);

        var sqlQueryText = $"SELECT * FROM {_eventsContainerId} e" +
                           " WHERE e.stream.id = @streamId" +
                           " ORDER BY e.stream.version";

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText)
            .WithParameter("@streamId", documentId);

        FeedIterator<EventWrapper> feedIterator = eventContainer.GetItemQueryIterator<EventWrapper>(
            queryDefinition,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
        );
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await feedIterator.ReadNextAsync();
            foreach (var eventWrapper in response)
            {
                var @event = eventWrapper.GetEvent();
                await EventStoreOnEventAdded(@event);
            }
        }
    }

    private async Task HandleChangesAsync(IReadOnlyCollection<Change> changes, CancellationToken cancellationToken)
    {
        foreach (var change in changes)
        {
            var @event = change.GetEvent();

            await EventStoreOnEventAdded(@event);
        }
    }
}