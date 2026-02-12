using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Tests.Domain.Events;

public record BulkOrderTagChanged : CrossAggregateEvent
{
    public BulkOrderTagChanged() { }

    public BulkOrderTagChanged(string aggregateType, string newTag, string targetPartitionKey)
    {
        AggregateType = aggregateType;
        NewTag = newTag;
        TargetPartitionKey = targetPartitionKey;
    }

    public string NewTag { get; init; } = string.Empty;
}
