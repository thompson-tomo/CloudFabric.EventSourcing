using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Net;
using System.Text.Json;
using CloudFabric.EventSourcing.EventStore.Persistence;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace CloudFabric.EventSourcing.EventStore.CosmosDb;

public class CosmosDbEventStore : IEventStore
{
    private readonly CosmosClient _client;
    private readonly bool _ownsClient;
    private readonly string _eventsContainerId;
    private readonly string _databaseId;

    public CosmosDbEventStore(
        string connectionString,
        CosmosClientOptions cosmosClientOptions,
        string databaseId,
        string eventsContainerId
    )
    {
        _client = new CosmosClient(connectionString, cosmosClientOptions);
        _ownsClient = true;
        _databaseId = databaseId;
        _eventsContainerId = eventsContainerId;
    }

    public CosmosDbEventStore(
        CosmosClient client,
        string databaseId,
        string eventsContainerId
    )
    {
        _client = client;
        _ownsClient = false;
        _databaseId = databaseId;
        _eventsContainerId = eventsContainerId;
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
        {
            _client.Dispose();
        }

        return ValueTask.CompletedTask;
    }

    public Task Initialize(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public async Task<EventStoreStatistics> GetStatistics(CancellationToken cancellationToken = default)
    {
        Container eventContainer = _client.GetContainer(_databaseId, _eventsContainerId);

        return await GetStatistics(eventContainer);
    }
    
    protected async Task<EventStoreStatistics> GetStatistics(Container eventsContainer)
    {
        var stats = new EventStoreStatistics();

        IOrderedQueryable<EventWrapper> totalCountQueryable = eventsContainer.GetItemLinqQueryable<EventWrapper>();
        stats.TotalEventsCount = await totalCountQueryable.CountAsync();

        QueryDefinition firstEventQuery = new QueryDefinition(
            $"SELECT * FROM {eventsContainer.Id} e ORDER BY e._ts ASC LIMIT 1"
        );
        FeedIterator<EventWrapper> firstEventFeedIterator = eventsContainer.GetItemQueryIterator<EventWrapper>(
            firstEventQuery,
            requestOptions: new QueryRequestOptions { }
        );
        while (firstEventFeedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await firstEventFeedIterator.ReadNextAsync();

            if (response.Count > 0)
            {
                stats.FirstEventCreatedAt = response.First().GetEvent().Timestamp;
            }
        }
        
        QueryDefinition lastEventQuery = new QueryDefinition(
            $"SELECT * FROM {eventsContainer.Id} e ORDER BY e._ts DESC LIMIT 1"
        );
        FeedIterator<EventWrapper> lastEventFeedIterator = eventsContainer.GetItemQueryIterator<EventWrapper>(
            lastEventQuery,
            requestOptions: new QueryRequestOptions { }
        );
        while (lastEventFeedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await lastEventFeedIterator.ReadNextAsync();

            if (response.Count > 0)
            {
                stats.LastEventCreatedAt = response.First().GetEvent().Timestamp;
            }
        }

        return stats;
    }

    public async Task DeleteAll(CancellationToken cancellationToken = default)
    {
        if (_client == null)
        {
            return;
        }

        var container = _client.GetContainer(_databaseId, _eventsContainerId);

        if (container != null)
        {
            try
            {
                await container.DeleteContainerAsync(cancellationToken: cancellationToken);
            }
            catch (CosmosException ex)
            {
                if (ex.StatusCode != System.Net.HttpStatusCode.NotFound)
                {
                    throw;
                }
            }
        }
    }

    public async Task<bool> HardDeleteAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var container = _client.GetContainer(_databaseId, _eventsContainerId);

        var sqlQueryText = $"SELECT * FROM {_eventsContainerId} e" + " WHERE e.stream.id = @streamId";

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText)
            .WithParameter("@streamId", streamId);

        PartitionKey searchedPartitionKey = new PartitionKey(partitionKey);

        FeedIterator<EventWrapper> feedIterator = container.GetItemQueryIterator<EventWrapper>(
            queryDefinition,
            requestOptions: new QueryRequestOptions { PartitionKey = searchedPartitionKey }
        );

        List<TransactionalBatch> batches = new List<TransactionalBatch>
        {
            container.CreateTransactionalBatch(searchedPartitionKey)
        };

        var recordsIdCounter = new List<Guid>();

        // TO DO: Implement container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("string")).
        // If we will use CloudFabric.EAV - partiton key in inherited from AgrefateBase classes
        // should be constructed like "Id"+"Entity" for this implementation
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await feedIterator.ReadNextAsync();
            foreach (var eventWrapper in response)
            {
                batches.Last().DeleteItem(eventWrapper.Id.ToString());

                recordsIdCounter.Add(eventWrapper.Id!.Value);

                // Use max count of operations 100 due to the transactional batch limits.
                // For more info see https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=dotnet.
                if (recordsIdCounter.Count == 100)
                {
                    batches.Add(container.CreateTransactionalBatch(searchedPartitionKey));

                    recordsIdCounter.Clear();
                }
            }
        }

        var transactionalResults = new ConcurrentBag<TransactionalBatchResponse>();

        await Parallel.ForEachAsync(batches, cancellationToken, async (batch, ct) =>
            {
                transactionalResults.Add(await batch.ExecuteAsync(ct).ConfigureAwait(false));
            }
        ).ConfigureAwait(false);

        if (transactionalResults.Any(x => !x.IsSuccessStatusCode))
        {
            return false;
        }

        return true;
    }

    public async Task<EventStream> LoadStreamAsyncOrThrowNotFound(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        Container container = _client.GetContainer(_databaseId, _eventsContainerId);

        var sqlQueryText = $"SELECT * FROM {_eventsContainerId} e" + " WHERE e.stream.id = @streamId " + " ORDER BY e.stream.version";

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText)
            .WithParameter("@streamId", streamId);

        int version = 0;
        var events = new List<IEvent>();

        FeedIterator<EventWrapper> feedIterator = container.GetItemQueryIterator<EventWrapper>(
            queryDefinition,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
        );
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await feedIterator.ReadNextAsync(cancellationToken);
            foreach (var eventWrapper in response)
            {
                version = eventWrapper.StreamInfo.Version;

                events.Add(eventWrapper.GetEvent());
            }
        }

        if (events.Count == 0)
        {
            throw new NotFoundException();
        }

        return new EventStream(streamId, version, events);
    }

    public async Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        Container container = _client.GetContainer(_databaseId, _eventsContainerId);

        var sqlQueryText = $"SELECT * FROM {_eventsContainerId} e" + " WHERE e.stream.id = @streamId " + " ORDER BY e.stream.version";

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText)
            .WithParameter("@streamId", streamId);

        int version = 0;
        var events = new List<IEvent>();

        FeedIterator<EventWrapper> feedIterator = container.GetItemQueryIterator<EventWrapper>(
            queryDefinition,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
        );
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await feedIterator.ReadNextAsync(cancellationToken);
            foreach (var eventWrapper in response)
            {
                version = eventWrapper.StreamInfo.Version;

                events.Add(eventWrapper.GetEvent());
            }
        }

        return new EventStream(streamId, version, events);
    }

    public async Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, int fromVersion, CancellationToken cancellationToken = default)
    {
        Container container = _client.GetContainer(_databaseId, _eventsContainerId);

        var sqlQueryText = $"SELECT * FROM {_eventsContainerId} e" +
                           " WHERE e.stream.id = @streamId AND e.stream.version >= @fromVersion" +
                           " ORDER BY e.stream.version";

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText)
            .WithParameter("@streamId", streamId)
            .WithParameter("@fromVersion", fromVersion);

        int version = 0;
        var events = new List<IEvent>();

        FeedIterator<EventWrapper> feedIterator = container.GetItemQueryIterator<EventWrapper>(
            queryDefinition,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
        );
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<EventWrapper> response = await feedIterator.ReadNextAsync(cancellationToken);
            foreach (var eventWrapper in response)
            {
                version = eventWrapper.StreamInfo.Version;
                events.Add(eventWrapper.GetEvent());
            }
        }

        return new EventStream(streamId, version, events);
    }

    public async Task<bool> AppendToStreamAsync(
        EventUserInfo eventUserInfo,
        Guid streamId,
        int expectedVersion,
        IEnumerable<IEvent> events,
        CancellationToken cancellationToken = default
    )
    {
        var eventsList = events as IList<IEvent> ?? events.ToList();

        if (eventsList.GroupBy(x => x.PartitionKey).Count() != 1)
        {
            throw new ArgumentException("Partition keys for all events in the stream must be the same");
        }

        Container container = _client.GetContainer(_databaseId, _eventsContainerId);

        PartitionKey cosmosPartitionKey = new PartitionKey(eventsList[0].PartitionKey);

        dynamic[] parameters = new dynamic[]
        {
            streamId,
            expectedVersion,
            SerializeEvents(eventUserInfo, streamId, expectedVersion, eventsList)
        };

        return await container.Scripts.ExecuteStoredProcedureAsync<bool>("spAppendToStream", cosmosPartitionKey, parameters, cancellationToken: cancellationToken);
    }

    private static string SerializeEvents(
        EventUserInfo eventUserInfo,
        Guid streamId,
        int expectedVersion,
        IEnumerable<IEvent> events
    )
    {
        if (eventUserInfo.UserId == Guid.Empty)
            throw new Exception("UserInfo.Id must be set to a value.");

        var items = events.Select(
            e => new EventWrapper
            {
                Id = Guid.NewGuid(),
                StreamInfo = new StreamInfo
                {
                    Id = streamId,
                    Version = ++expectedVersion
                },
                EventType = e.GetType().AssemblyQualifiedName,
                EventData = JsonSerializer.SerializeToElement(e, e.GetType(), EventStoreSerializerOptions.Options),
                UserInfo = JsonSerializer.SerializeToElement(eventUserInfo, eventUserInfo.GetType(), EventStoreSerializerOptions.Options)
            }
        );

        return JsonSerializer.Serialize(items, EventStoreSerializerOptions.Options);
    }
    
    
    public async Task<LoadEventsResult> LoadEventsAsync(
        string? partitionKey,
        DateTime? dateFrom = null,
        int chunkSize = 250,
        string? continuationToken = null,
        CancellationToken cancellationToken = default
    ) {
        Container eventContainer = _client.GetContainer(_databaseId, _eventsContainerId);

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

        var results = new List<IEvent>();

        while (feedIterator.HasMoreResults)
        {
            FeedResponse<Change> response = await feedIterator.ReadNextAsync(cancellationToken);

            if (response.All(x => x.GetEvent().Timestamp > endTime))
            {
                break;
            }

            if (response.StatusCode != HttpStatusCode.NotModified)
            {
                var events = new ReadOnlyCollection<Change>(response.ToList());

                results.AddRange(events.Select(e => e.GetEvent()));
            }
        }

        return new LoadEventsResult
        {
            Events = results,
            ContinuationToken = null // CosmosDb change feed handles its own pagination
        };
    }

    public Task<bool> AppendGlobalEventAsync(
        EventUserInfo eventUserInfo,
        ICrossAggregateEvent @event,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotImplementedException("Cross-aggregate events are not yet supported for CosmosDb backend.");
    }

    public Task<List<IEvent>> LoadGlobalEventsAsync(
        string aggregateType,
        string? partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        throw new NotImplementedException("Cross-aggregate events are not yet supported for CosmosDb backend.");
    }
}