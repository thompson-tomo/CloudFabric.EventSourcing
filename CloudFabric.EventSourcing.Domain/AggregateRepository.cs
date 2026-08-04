using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;

namespace CloudFabric.EventSourcing.Domain;

public class AggregateRepository<T> : IAggregateRepository<T> where T : AggregateBase
{
    private readonly IEventStore _eventStore;
    private readonly IAggregateSnapshotStore? _snapshotStore;

    /// <summary>
    /// Number of own events between snapshot saves. When the aggregate's version after a successful
    /// <see cref="SaveAsync"/> is a multiple of this threshold, a new snapshot is persisted.
    /// Ignored when <paramref name="snapshotStore"/> is null or the aggregate does not support snapshots.
    /// </summary>
    private readonly int _snapshotThreshold;

    public AggregateRepository(IEventStore eventStore)
        : this(eventStore, null, 50)
    {
    }

    public AggregateRepository(IEventStore eventStore, IAggregateSnapshotStore? snapshotStore, int snapshotThreshold = 50)
    {
        _eventStore = eventStore;
        _snapshotStore = snapshotStore;
        _snapshotThreshold = snapshotThreshold;
    }

    public async Task<T?> LoadAsync(Guid id, string partitionKey, CancellationToken cancellationToken = default)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        // Try snapshot-based load first if a snapshot store is configured.
        if (_snapshotStore != null)
        {
            var fromSnapshot = await TryLoadFromSnapshotAsync(id, partitionKey, cancellationToken);
            if (fromSnapshot != null) return fromSnapshot;
        }

        // Fall back to full event replay.
        var eventStream = await _eventStore.LoadStreamAsync(id, partitionKey, cancellationToken);

        if (!eventStream.Events.Any()) return null;

        var mergedEvents = await MergeWithGlobalEvents(eventStream, partitionKey, cancellationToken);
        return ConstructAggregateInstanceFromEvents(eventStream, mergedEvents);
    }

    public async Task<T> LoadAsyncOrThrowNotFound(Guid id, string partitionKey, CancellationToken cancellationToken = default)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        // Try snapshot-based load first if a snapshot store is configured.
        if (_snapshotStore != null)
        {
            var fromSnapshot = await TryLoadFromSnapshotAsync(id, partitionKey, cancellationToken);
            if (fromSnapshot != null) return fromSnapshot;
        }

        // Fall back to full event replay.
        var eventStream = await _eventStore.LoadStreamAsyncOrThrowNotFound(id, partitionKey, cancellationToken);

        var mergedEvents = await MergeWithGlobalEvents(eventStream, partitionKey, cancellationToken);
        return ConstructAggregateInstanceFromEvents(eventStream, mergedEvents);
    }

    /// <summary>
    /// Attempts to load the aggregate from a snapshot + remaining events.
    /// Returns null if no snapshot is available or the aggregate type does not support snapshots,
    /// indicating that the caller should fall back to full event replay.
    /// </summary>
    private async Task<T?> TryLoadFromSnapshotAsync(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken)
    {
        var snapshot = await _snapshotStore!.LoadLatestSnapshotAsync(id, partitionKey, cancellationToken);
        if (snapshot == null) return null;

        // Resolve the concrete aggregate type from the snapshot metadata.
        var type = Type.GetType(snapshot.AggregateType, AggregateTypeAssemblyResolver, null) ?? typeof(T);

        // Create an aggregate instance via the default constructor to check snapshot support.
        T? aggregate;
        try
        {
            aggregate = (T?)Activator.CreateInstance(type);
        }
        catch
        {
            // No default constructor — cannot restore from snapshot; fall back to full replay.
            return null;
        }

        if (aggregate == null || !aggregate.SupportsSnapshots) return null;

        // Restore state from the snapshot.
        aggregate.InitFromSnapshot(snapshot.StateJson, snapshot.Version, snapshot.LastAppliedEventTimestamp);

        // Load own events that occurred after the snapshot version.
        var remainingOwnStream = await _eventStore.LoadStreamAsync(
            id, partitionKey, snapshot.Version + 1, cancellationToken);

        // Load global (cross-aggregate) events, keeping only those that postdate the snapshot.
        var allGlobalEvents = await _eventStore.LoadGlobalEventsAsync(
            snapshot.AggregateType, partitionKey, cancellationToken);

        var newGlobalEvents = allGlobalEvents
            .Where(e => e.Timestamp > snapshot.LastAppliedEventTimestamp)
            .ToList();

        // Merge remaining own events and new global events by timestamp, then replay.
        var remainingEvents = remainingOwnStream.Events
            .Concat(newGlobalEvents)
            .OrderBy(e => e.Timestamp);

        foreach (var @event in remainingEvents)
        {
            aggregate.ApplyHistoricalEvent(@event);
        }

        return aggregate;
    }

    /// <summary>
    /// Loads global (cross-aggregate) events for the aggregate's type and merges them with
    /// the aggregate's own events, ordered by timestamp. This allows the aggregate to apply
    /// bulk changes that affect it.
    /// </summary>
    private async Task<IEnumerable<IEvent>> MergeWithGlobalEvents(
        EventStream eventStream, string partitionKey, CancellationToken cancellationToken)
    {
        if (!eventStream.Events.Any()) return eventStream.Events;

        // Determine aggregate type from the first event
        var firstEvent = eventStream.Events.First();
        if (string.IsNullOrEmpty(firstEvent.AggregateType))
        {
            return eventStream.Events;
        }

        var globalEvents = await _eventStore.LoadGlobalEventsAsync(
            firstEvent.AggregateType, partitionKey, cancellationToken);

        if (globalEvents.Count == 0)
        {
            return eventStream.Events;
        }

        // Merge by timestamp
        return eventStream.Events
            .Concat(globalEvents)
            .OrderBy(e => e.Timestamp)
            .ToList();
    }

    private T ConstructAggregateInstanceFromEvents(EventStream eventStream, IEnumerable<IEvent> events)
    {
        var firstEvent = eventStream.Events.First();

        // Support for derived types. The construction of generic T here will not work if
        // our aggregate is one of many derived types of T.
        // Hence we are storing exact aggregate type in each event to be able to construct
        // exact derived type.
        if (!string.IsNullOrEmpty(firstEvent.AggregateType))
        {
            var type = Type.GetType(firstEvent.AggregateType, AggregateTypeAssemblyResolver, null);

            if (type != null)
            {
                return (T?)Activator.CreateInstance(type, new object[] { events }) ??
                       throw new InvalidOperationException(
                           "Unable to construct Aggregate instance of type " +
                           $"{firstEvent.AggregateType}"
                       );
            }
        }

        return (T?)Activator.CreateInstance(typeof(T), new object[] { events }) ??
               throw new InvalidOperationException(
                   "Unable to construct aggregate instance of type " +
                   $"{typeof(T).AssemblyQualifiedName}"
               );
    }

    private static System.Reflection.Assembly AggregateTypeAssemblyResolver(System.Reflection.AssemblyName assemblyName)
    {
        assemblyName.Version = null;
        return System.Reflection.Assembly.Load(assemblyName);
    }

    public async Task<bool> SaveAsync(EventUserInfo eventUserInfo, T aggregate, CancellationToken cancellationToken = default)
    {
        if (aggregate.UncommittedEvents.Any())
        {
            var streamId = aggregate.Id;

            foreach (var e in aggregate.UncommittedEvents)
            {
                e.AggregateType = aggregate.GetType().AssemblyQualifiedName ?? "";
            }

            // Capture the max timestamp of the uncommitted events before they are cleared by OnChangesSaved().
            // This is used to compute the snapshot's LastAppliedEventTimestamp.
            var uncommittedMaxTimestamp = aggregate.UncommittedEvents.Max(e => e.Timestamp);

            var eventsSavedSuccessfully = await _eventStore.AppendToStreamAsync(
                eventUserInfo,
                streamId,
                aggregate.Version,
                aggregate.UncommittedEvents,
                cancellationToken
            );

            if (eventsSavedSuccessfully)
            {
                aggregate.OnChangesSaved();

                // Optionally persist a snapshot after every N own events.
                if (_snapshotStore != null
                    && aggregate.SupportsSnapshots
                    && _snapshotThreshold > 0
                    && aggregate.Version % _snapshotThreshold == 0)
                {
                    try
                    {
                        // LastAppliedEventTimestamp is the later of:
                        //   (a) the max timestamp of events applied during the last LoadAsync, and
                        //   (b) the max timestamp of the events just committed.
                        var snapshotTimestamp = uncommittedMaxTimestamp > aggregate.LastAppliedEventTimestamp
                            ? uncommittedMaxTimestamp
                            : aggregate.LastAppliedEventTimestamp;

                        await _snapshotStore.SaveSnapshotAsync(
                            new AggregateSnapshot
                            {
                                StreamId = aggregate.Id,
                                PartitionKey = aggregate.PartitionKey,
                                AggregateType = aggregate.GetType().AssemblyQualifiedName ?? "",
                                Version = aggregate.Version,
                                StateJson = aggregate.CreateSnapshot(),
                                LastAppliedEventTimestamp = snapshotTimestamp
                            },
                            cancellationToken
                        );
                    }
                    catch
                    {
                        // A snapshot save failure is non-fatal: the aggregate events were committed
                        // successfully and the next load will still work via full event replay.
                    }
                }
            }

            return eventsSavedSuccessfully;
        }

        return true;
    }

    /// <summary>
    /// Batch-saves multiple NEW aggregates via <see cref="IEventStore.AppendNewStreamsAsync"/>.
    /// All aggregates must be new (version 0). Events are committed in a single transaction,
    /// then handlers are notified for projection building.
    /// </summary>
    public async Task SaveMultipleNewAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<T> aggregates,
        CancellationToken cancellationToken = default)
    {
        var streams = aggregates.Select(a =>
        {
            foreach (var e in a.UncommittedEvents)
            {
                e.AggregateType = a.GetType().AssemblyQualifiedName ?? "";
            }
            return (a.Id, a.PartitionKey, (IReadOnlyList<IEvent>)a.UncommittedEvents.ToList());
        }).ToList();

        await _eventStore.AppendNewStreamsAsync(eventUserInfo, streams, cancellationToken);

        foreach (var a in aggregates)
        {
            a.OnChangesSaved();
        }
    }

    /// <inheritdoc />
    public async Task AppendEventsToMultipleAsync(
        EventUserInfo eventUserInfo,
        IReadOnlyList<(Guid StreamId, string PartitionKey, IReadOnlyList<IEvent> Events)> streams,
        CancellationToken cancellationToken = default)
    {
        var aggregateType = typeof(T).AssemblyQualifiedName ?? "";
        foreach (var (_, _, events) in streams)
        {
            foreach (var e in events)
            {
                e.AggregateType = aggregateType;
            }
        }

        await _eventStore.AppendToMultipleExistingStreamsAsync(eventUserInfo, streams, cancellationToken);
    }

    /// <summary>
    /// We should not be able to hard delete events within implementation, but for some development issues we do need to be able to do so.
    /// Use this method carefully and at your own risk. When something went terribly wrong, there is no way to recover deleted data.
    /// </summary>
    public async Task<bool> HardDeleteAsync(Guid id, string partitionKey, CancellationToken cancellationToken = default)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        return await _eventStore.HardDeleteAsync(id, partitionKey, cancellationToken);
    }
}
