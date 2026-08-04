using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Tests.Domain.Events;

public record OrderCancelled : Event
{
    public OrderCancelled() { }

    public OrderCancelled(Guid id, string partitionKey)
    {
        AggregateId = id;
        PartitionKey = partitionKey;
    }
}
