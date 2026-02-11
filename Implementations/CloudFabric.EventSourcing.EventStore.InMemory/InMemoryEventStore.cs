using System.Collections.Concurrent;
using System.Text.Json;
using CloudFabric.EventSourcing.EventStore.Persistence;

namespace CloudFabric.EventSourcing.EventStore.InMemory;

public class InMemoryEventStore : IEventStore
{
    private readonly ConcurrentDictionary<(Guid StreamId, string PartitionKey), List<string>> _eventsContainer;
    private readonly object _lock = new();
    private readonly List<Func<IEvent, Task>> _eventAddedEventHandlers = new();

    public InMemoryEventStore(
        ConcurrentDictionary<(Guid StreamId, string PartitionKey), List<string>> eventsContainer
    )
    {
        _eventsContainer = eventsContainer;
    }

    public Task Initialize(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public void SubscribeToEventAdded(Func<IEvent, Task> handler)
    {
        lock (_lock)
        {
            _eventAddedEventHandlers.Add(handler);
        }
    }

    public void UnsubscribeFromEventAdded(Func<IEvent, Task> handler)
    {
        lock (_lock)
        {
            _eventAddedEventHandlers.Remove(handler);
        }
    }

    public Task<EventStoreStatistics> GetStatistics(CancellationToken cancellationToken = default)
    {
        var stats = new EventStoreStatistics();

        DateTime? firstTimestamp = null;
        DateTime? lastTimestamp = null;
        long totalCount = 0;

        foreach (var kvp in _eventsContainer)
        {
            List<string> snapshot;
            lock (_lock)
            {
                snapshot = kvp.Value.ToList();
            }

            totalCount += snapshot.Count;

            foreach (var data in snapshot)
            {
                var wrapper = JsonSerializer.Deserialize<EventWrapper>(data, EventStoreSerializerOptions.Options);
                if (wrapper == null) continue;

                var evt = wrapper.GetEvent();
                if (firstTimestamp == null || evt.Timestamp < firstTimestamp)
                {
                    firstTimestamp = evt.Timestamp;
                }
                if (lastTimestamp == null || evt.Timestamp > lastTimestamp)
                {
                    lastTimestamp = evt.Timestamp;
                }
            }
        }

        stats.TotalEventsCount = totalCount;
        if (firstTimestamp.HasValue) stats.FirstEventCreatedAt = firstTimestamp.Value;
        if (lastTimestamp.HasValue) stats.LastEventCreatedAt = lastTimestamp.Value;

        return Task.FromResult(stats);
    }

    public Task DeleteAll(CancellationToken cancellationToken = default)
    {
        _eventsContainer.Clear();
        return Task.CompletedTask;
    }

    public Task<bool> HardDeleteAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var result = _eventsContainer.TryRemove((streamId, partitionKey), out _);
        return Task.FromResult(result);
    }

    public Task<EventStream> LoadStreamAsyncOrThrowNotFound(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var eventWrappers = LoadOrderedEventWrappers(streamId, partitionKey);
        if (eventWrappers.Count == 0)
        {
            throw new NotFoundException();
        }

        int version = eventWrappers.Max(x => x.StreamInfo.Version);
        var events = eventWrappers.Select(w => w.GetEvent()).ToList();

        return Task.FromResult(new EventStream(streamId, version, events));
    }

    public Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var eventWrappers = LoadOrderedEventWrappers(streamId, partitionKey);

        int version = eventWrappers.Count > 0
            ? eventWrappers.Max(x => x.StreamInfo.Version)
            : 0;
        var events = eventWrappers.Select(w => w.GetEvent()).ToList();

        return Task.FromResult(new EventStream(streamId, version, events));
    }

    public Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, int fromVersion, CancellationToken cancellationToken = default)
    {
        var eventWrappers = LoadOrderedEventWrappersFromVersion(streamId, partitionKey, fromVersion);

        int version = eventWrappers.Count > 0
            ? eventWrappers.Max(x => x.StreamInfo.Version)
            : 0;
        var events = eventWrappers.Select(w => w.GetEvent()).ToList();

        return Task.FromResult(new EventStream(streamId, version, events));
    }

    public Task<LoadEventsResult> LoadEventsAsync(
        string? partitionKey,
        DateTime? dateFrom = null,
        int limit = 250,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        var snapshot = _eventsContainer.ToArray();

        if (snapshot.Length == 0)
        {
            return Task.FromResult(new LoadEventsResult());
        }

        var filtered = !string.IsNullOrEmpty(partitionKey)
            ? snapshot.Where(x => x.Key.PartitionKey == partitionKey)
            : snapshot;

        // Deserialize all wrappers to get both Id and Event
        var wrappers = filtered
            .SelectMany(x => x.Value)
            .Select(x => JsonSerializer.Deserialize<EventWrapper>(x, EventStoreSerializerOptions.Options)!)
            .OrderBy(x => x.GetEvent().Timestamp)
            .ThenBy(x => x.Id)
            .AsEnumerable();

        // Apply cursor-based pagination if continuation token is present
        if (!string.IsNullOrEmpty(continuationToken) && TryParseContinuationToken(continuationToken, out var cursorTimestamp, out var cursorId))
        {
            wrappers = wrappers.Where(w =>
            {
                var ts = w.GetEvent().Timestamp;
                return ts > cursorTimestamp || (ts == cursorTimestamp && Comparer<Guid?>.Default.Compare(w.Id, cursorId) > 0);
            });
        }
        else if (dateFrom.HasValue)
        {
            wrappers = wrappers.Where(w => w.GetEvent().Timestamp >= dateFrom.Value);
        }

        var page = wrappers.Take(limit).ToList();
        var events = page.Select(w => w.GetEvent()).ToList();

        var lastWrapper = page.LastOrDefault();
        string? nextToken = null;
        if (lastWrapper != null)
        {
            nextToken = BuildContinuationToken(lastWrapper.GetEvent().Timestamp, lastWrapper.Id!.Value);
        }

        return Task.FromResult(new LoadEventsResult
        {
            Events = events,
            ContinuationToken = nextToken
        });
    }

    private static string BuildContinuationToken(DateTime timestamp, Guid id)
    {
        return $"{timestamp:O}|{id}";
    }

    private static bool TryParseContinuationToken(string token, out DateTime timestamp, out Guid id)
    {
        timestamp = default;
        id = default;

        var parts = token.Split('|', 2);
        if (parts.Length != 2)
        {
            return false;
        }

        return DateTime.TryParse(parts[0], System.Globalization.CultureInfo.InvariantCulture,
                   System.Globalization.DateTimeStyles.RoundtripKind, out timestamp)
               && Guid.TryParse(parts[1], out id);
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

        var partitionKey = eventsList[0].PartitionKey;

        lock (_lock)
        {
            _eventsContainer.TryGetValue((streamId, partitionKey), out var currentStream);
            var currentVersion = 0;
            if (currentStream != null && currentStream.Count > 0)
            {
                currentVersion = currentStream
                    .Select(s => JsonSerializer.Deserialize<EventWrapper>(s, EventStoreSerializerOptions.Options)!)
                    .Max(w => w.StreamInfo.Version);
            }

            if (currentVersion != expectedVersion)
            {
                return false;
            }

            var wrappers = PrepareEvents(eventUserInfo, streamId, expectedVersion, eventsList);
            var stream = currentStream ?? new List<string>();

            foreach (var wrapper in wrappers)
            {
                stream.Add(JsonSerializer.Serialize(wrapper, EventStoreSerializerOptions.Options));
            }

            _eventsContainer[(streamId, partitionKey)] = stream;
        }

        List<Func<IEvent, Task>> handlers;
        lock (_lock)
        {
            handlers = _eventAddedEventHandlers.ToList();
        }

        foreach (var e in eventsList)
        {
            foreach (var h in handlers)
            {
                await h(e);
            }
        }

        return true;
    }

    private List<EventWrapper> LoadOrderedEventWrappers(Guid streamId, string partitionKey)
    {
        if (!_eventsContainer.TryGetValue((streamId, partitionKey), out var eventData))
        {
            return new List<EventWrapper>();
        }

        List<string> snapshot;
        lock (_lock)
        {
            snapshot = eventData.ToList();
        }

        return snapshot
            .Select(data => JsonSerializer.Deserialize<EventWrapper>(data, EventStoreSerializerOptions.Options)!)
            .OrderBy(x => x.StreamInfo.Version)
            .ToList();
    }

    private List<EventWrapper> LoadOrderedEventWrappersFromVersion(Guid streamId, string partitionKey, int version)
    {
        if (!_eventsContainer.TryGetValue((streamId, partitionKey), out var eventData))
        {
            return new List<EventWrapper>();
        }

        List<string> snapshot;
        lock (_lock)
        {
            snapshot = eventData.ToList();
        }

        return snapshot
            .Select(data => JsonSerializer.Deserialize<EventWrapper>(data, EventStoreSerializerOptions.Options)!)
            .Where(w => w.StreamInfo.Version >= version)
            .OrderBy(x => x.StreamInfo.Version)
            .ToList();
    }

    private static List<EventWrapper> PrepareEvents(
        EventUserInfo eventUserInfo, Guid streamId, int expectedVersion, IList<IEvent> events
    )
    {
        if (eventUserInfo.UserId == Guid.Empty)
            throw new Exception("UserInfo.Id must be set to a value.");

        var wrappers = new List<EventWrapper>(events.Count);
        foreach (var e in events)
        {
            wrappers.Add(new EventWrapper
            {
                Id = Guid.NewGuid(),
                StreamInfo = new StreamInfo { Id = streamId, Version = ++expectedVersion },
                EventType = e.GetType().AssemblyQualifiedName,
                EventData = JsonSerializer.SerializeToElement(e, e.GetType(), EventStoreSerializerOptions.Options),
                UserInfo = JsonSerializer.SerializeToElement(eventUserInfo, eventUserInfo.GetType(), EventStoreSerializerOptions.Options)
            });
        }

        return wrappers;
    }

    public async Task<bool> AppendGlobalEventAsync(
        EventUserInfo eventUserInfo,
        ICrossAggregateEvent @event,
        CancellationToken cancellationToken = default
    )
    {
        var streamId = DeterministicGuid.Create(@event.AggregateType);
        var partitionKey = @event.TargetPartitionKey ?? CrossAggregateEvent.AllPartitionsKey;

        @event.PartitionKey = partitionKey;

        lock (_lock)
        {
            _eventsContainer.TryGetValue((streamId, partitionKey), out var currentStream);
            var currentVersion = 0;
            if (currentStream != null && currentStream.Count > 0)
            {
                currentVersion = currentStream
                    .Select(s => JsonSerializer.Deserialize<EventWrapper>(s, EventStoreSerializerOptions.Options)!)
                    .Max(w => w.StreamInfo.Version);
            }

            var wrapper = new EventWrapper
            {
                Id = Guid.NewGuid(),
                StreamInfo = new StreamInfo { Id = streamId, Version = currentVersion + 1 },
                EventType = @event.GetType().AssemblyQualifiedName,
                EventData = JsonSerializer.SerializeToElement(@event, @event.GetType(), EventStoreSerializerOptions.Options),
                UserInfo = JsonSerializer.SerializeToElement(eventUserInfo, eventUserInfo.GetType(), EventStoreSerializerOptions.Options)
            };

            var stream = currentStream ?? new List<string>();
            stream.Add(JsonSerializer.Serialize(wrapper, EventStoreSerializerOptions.Options));
            _eventsContainer[(streamId, partitionKey)] = stream;
        }

        List<Func<IEvent, Task>> handlers;
        lock (_lock)
        {
            handlers = _eventAddedEventHandlers.ToList();
        }

        foreach (var h in handlers)
        {
            await h(@event);
        }

        return true;
    }

    public Task<List<IEvent>> LoadGlobalEventsAsync(
        string aggregateType,
        string? partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        var streamId = DeterministicGuid.Create(aggregateType);
        var events = new List<IEvent>();

        List<(Guid StreamId, string PartitionKey)> matchingKeys;

        if (partitionKey != null)
        {
            matchingKeys = _eventsContainer.Keys
                .Where(k => k.StreamId == streamId &&
                            (k.PartitionKey == partitionKey || k.PartitionKey == CrossAggregateEvent.AllPartitionsKey))
                .ToList();
        }
        else
        {
            matchingKeys = _eventsContainer.Keys
                .Where(k => k.StreamId == streamId)
                .ToList();
        }

        foreach (var key in matchingKeys)
        {
            if (!_eventsContainer.TryGetValue(key, out var eventData))
            {
                continue;
            }

            List<string> snapshot;
            lock (_lock)
            {
                snapshot = eventData.ToList();
            }

            var wrappers = snapshot
                .Select(data => JsonSerializer.Deserialize<EventWrapper>(data, EventStoreSerializerOptions.Options)!)
                .ToList();

            events.AddRange(wrappers.Select(w => w.GetEvent()));
        }

        events.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

        return Task.FromResult(events);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
