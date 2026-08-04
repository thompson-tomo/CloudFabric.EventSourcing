using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.Projections;
using CloudFabric.Projections.Queries;

namespace CloudFabric.EventSourcing.Tests.Domain.Projections.OrdersListProjection;

public class OrdersListProjectionBuilder : ProjectionBuilder<OrderListProjectionItem>,
    IHandleEvent<OrderPlaced>,
    IHandleEvent<OrderItemAdded>,
    IHandleEvent<OrderItemRemoved>,
    IHandleEvent<OrderNameUpdated>,
    IHandleEvent<OrderCancelled>,
    IHandleEvent<AggregateUpdatedEvent<Order>>,
    IHandleCrossAggregateEvent<BulkOrderTagChanged>
{
    public OrdersListProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory, 
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write) 
        : base(projectionRepositoryFactory, indexSelector)
    {
    }

    public async Task On(OrderItemAdded evt)
    {
        await UpdateDocument(evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (orderProjection) => 
            {
                orderProjection.Items.Add(new OrderListProjectionOrderItem
                {
                    Name = evt.Item.Name,
                    Amount = evt.Item.Amount,
                    AddedAt = evt.Item.AddedAt
                });

                orderProjection.ItemsCount++;
            });
    }

    public async Task On(OrderItemRemoved evt)
    {
        await UpdateDocument(evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (orderProjection) =>
            {
                var item = orderProjection.Items.FirstOrDefault(x => x.Name == evt.Item.Name);

                if (item != null)
                {
                    orderProjection.Items.Remove(item);

                    orderProjection.ItemsCount--;
                }
            });
    }

    public async Task On(OrderPlaced evt)
    {
        var projectionItem = new OrderListProjectionItem()
        {
            Id = evt.AggregateId,
            Name = evt.OrderName,
            ItemsCount = evt.Items.Count
        };

        projectionItem.Items = evt.Items.Select(
            x =>
            new OrderListProjectionOrderItem
            {
                Amount = x.Amount,
                Name = x.Name,
                AddedAt = x.AddedAt
            }
        ).ToList();

        projectionItem.CreatedBy = new OrderListProjectionUserInfo
        {
            UserId = evt.CreatedById,
            Email = evt.CreatedByEmail
        };

        await UpsertDocument(projectionItem, evt.PartitionKey, evt.Timestamp);
    }

    public async Task On(OrderNameUpdated evt)
    {
        await UpdateDocument(evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (orderProjection) =>
            {
                orderProjection.Name = evt.NewOrderName;
            });
    }

    public async Task On(OrderCancelled evt)
    {
        await DeleteDocument(evt.AggregateId, evt.PartitionKey);
    }

    public async Task On(AggregateUpdatedEvent<Order> evt)
    {
        await SetDocumentUpdatedAt(evt.AggregateId, evt.PartitionKey, evt.UpdatedAt);
    }

    public async Task OnBulk(BulkOrderTagChanged evt)
    {
        await UpdateByQuery(
            new ProjectionQuery(),
            evt.PartitionKey,
            new Dictionary<string, object?> { { "Tag", evt.NewTag } },
            evt.Timestamp
        );
    }
}