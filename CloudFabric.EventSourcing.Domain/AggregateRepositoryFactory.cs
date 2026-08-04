using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Domain;

public class AggregateRepositoryFactory
{
    private readonly IEventStore _eventStore;
    private readonly IAggregateSnapshotStore? _snapshotStore;
    private readonly int _snapshotThreshold;

    public IEventStore EventStore => _eventStore;

    public AggregateRepositoryFactory(IEventStore eventStore)
        : this(eventStore, null, 50)
    {
    }

    /// <param name="eventStore">The underlying event store.</param>
    /// <param name="snapshotStore">
    /// Optional snapshot store. When provided, repositories will save and load snapshots
    /// for aggregates that opt in by overriding <see cref="AggregateBase.SupportsSnapshots"/>.
    /// </param>
    /// <param name="snapshotThreshold">
    /// Number of own events between snapshot saves (default: 50).
    /// A snapshot is persisted whenever the aggregate version after a successful save
    /// is a multiple of this value.
    /// </param>
    public AggregateRepositoryFactory(
        IEventStore eventStore,
        IAggregateSnapshotStore? snapshotStore,
        int snapshotThreshold = 50)
    {
        _eventStore = eventStore;
        _snapshotStore = snapshotStore;
        _snapshotThreshold = snapshotThreshold;
    }

    public AggregateRepository<TAggregate> GetAggregateRepository<TAggregate>() where TAggregate : AggregateBase
    {
        return new AggregateRepository<TAggregate>(_eventStore, _snapshotStore, _snapshotThreshold);
    }
}
