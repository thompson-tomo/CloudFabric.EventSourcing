using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;

namespace CloudFabric.EventSourcing.Tests;

public interface IOrderRepository
{
    Task<Order> LoadOrder(Guid id, string partitionKey);
    Task<bool> SaveOrder(EventUserInfo eventUserInfo, Order aggregate);
}

public class OrderRepository : IOrderRepository
{
    private readonly IEventStore _eventStore;

    public OrderRepository(
        IEventStore eventStore
    )
    {
        _eventStore = eventStore;
    }

    public async Task<Order> LoadOrder(Guid id, string partitionKey)
    {
        var stream = await _eventStore.LoadStreamAsyncOrThrowNotFound(id, partitionKey);
        return new Order(stream.Events);
    }

    public async Task<bool> SaveOrder(
        EventUserInfo eventUserInfo,
        Order aggregate
    )
    {
        if (aggregate.UncommittedEvents.Any())
        {
            var streamId = aggregate.Id;

            foreach (var e in aggregate.UncommittedEvents)
            {
                e.AggregateType = aggregate.GetType().AssemblyQualifiedName ?? "";
            }
            
            var savedEvents = await _eventStore.AppendToStreamAsync(eventUserInfo,
                streamId,
                aggregate.Version,
                aggregate.UncommittedEvents);

            if (savedEvents)
            {
                aggregate.OnChangesSaved();
            }

            return savedEvents;
        }

        return true;
    }
}