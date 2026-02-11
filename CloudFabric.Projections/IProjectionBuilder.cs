using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.Projections;

public interface IProjectionBuilder
{
    public HashSet<Type> HandledEventTypes { get; }

    Task ApplyEvent(IEvent @event);

    Task ApplyEvents(List<IEvent> events);

    Task ApplyCrossAggregateEvent(ICrossAggregateEvent @event);

    bool HandlesCrossAggregateEvent(Type eventType);
}

public interface IProjectionBuilder<TProjectionDocument> : IProjectionBuilder
    where TProjectionDocument : ProjectionDocument
{
}
