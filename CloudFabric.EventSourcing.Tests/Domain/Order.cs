using System.Collections.ObjectModel;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;

namespace CloudFabric.EventSourcing.Tests.Domain;

public class Order : AggregateBase
{
    public Order(IEnumerable<IEvent> events) : base(events)
    {
    }

    public Order(Guid id, string orderName, List<OrderItem> items, Guid createdById, string createdByEmail)
        : base(id)
    {
        Apply(new OrderPlaced(id, orderName, PartitionKey, items, createdById, createdByEmail));
    }

    public override string PartitionKey => PartitionKeys.GetOrderPartitionKey();

    public string OrderName { get; private set; } = string.Empty;

    /// <summary>
    /// It should not be possible to modify the collection from outside.
    /// The only way to modify the collection is by calling aggregate methods AddItem and RemoveItem.
    /// </summary>
    public ReadOnlyCollection<OrderItem> Items { get; private set; } = new ReadOnlyCollection<OrderItem>(Array.Empty<OrderItem>());
    public Guid CreatedById { get; private set; }
    public DateTime UpdatedAt { get; private set; }

    public void AddItem(OrderItem item)
    {
        Apply(new OrderItemAdded(Id, item, PartitionKey));
    }

    public void RemoveItem(string name)
    {
        var item = Items.FirstOrDefault(x => x.Name == name);
        
        if (item != null)
        {
            Apply(new OrderItemRemoved(Id, item, PartitionKey));
        }
    }

    #region Event Handlers

    public void On(OrderPlaced @event)
    {
        OrderName = @event.OrderName;
        Items = new ReadOnlyCollection<OrderItem>(@event.Items);
        CreatedById = @event.CreatedById;
        UpdatedAt = @event.Timestamp;
    }

    public void On(OrderItemAdded @event)
    {
        // build new list
        var items = new List<OrderItem>(Items) { @event.Item };
        // set to list with new item
        Items = items.AsReadOnly();
        UpdatedAt = @event.Timestamp;
    }

    public void On(OrderItemRemoved @event)
    {
        // build new list
        var items = new List<OrderItem>();
        items.AddRange(Items.Where(x => x.Name != @event.Item.Name));
        // set to list without item
        Items = items.AsReadOnly();
        UpdatedAt = @event.Timestamp;
    }

    #endregion
}