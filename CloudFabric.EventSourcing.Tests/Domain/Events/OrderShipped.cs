using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Tests.Domain.Events;

public record OrderShipped : Event
{
    public OrderShipped() { }

    public OrderShipped(Guid id, string trackingNumber, string partitionKey)
    {
        AggregateId = id;
        TrackingNumber = trackingNumber;
        PartitionKey = partitionKey;
    }

    public string TrackingNumber { get; init; } = string.Empty;
}
