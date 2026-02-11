using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.Projections;

/// <summary>
/// Implement this interface on a ProjectionBuilder to handle cross-aggregate events
/// via bulk UpdateByQuery operations instead of per-document updates.
/// </summary>
public interface IHandleCrossAggregateEvent<in TEvent> where TEvent : ICrossAggregateEvent
{
    Task OnBulk(TEvent @event);
}
