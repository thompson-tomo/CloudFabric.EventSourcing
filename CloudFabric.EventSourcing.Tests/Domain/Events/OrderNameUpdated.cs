using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Tests.Domain.Events;

public record OrderNameUpdated : Event
{
    public OrderNameUpdated() { }

    public OrderNameUpdated(Guid id, string newOrderName, string partitionKey)
    {
        AggregateId = id;
        NewOrderName = newOrderName;
        PartitionKey = partitionKey;
    }

    public string NewOrderName { get; init; } = string.Empty;
}
