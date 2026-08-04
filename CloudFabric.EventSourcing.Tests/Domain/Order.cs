using System.Collections.ObjectModel;
using System.Text.Json;
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
    public string Tag { get; private set; } = string.Empty;
    public bool IsCancelled { get; private set; }
    public string TrackingNumber { get; private set; } = string.Empty;

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

    public void Cancel()
    {
        Apply(new OrderCancelled(Id, PartitionKey));
    }

    public void Ship(string trackingNumber)
    {
        Apply(new OrderShipped(Id, trackingNumber, PartitionKey));
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

    public void On(OrderNameUpdated @event)
    {
        OrderName = @event.NewOrderName;
        UpdatedAt = @event.Timestamp;
    }

    public void On(BulkOrderTagChanged @event)
    {
        Tag = @event.NewTag;
        UpdatedAt = @event.Timestamp;
    }

    public void On(OrderCancelled @event)
    {
        IsCancelled = true;
        UpdatedAt = @event.Timestamp;
    }

    public void On(OrderShipped @event)
    {
        TrackingNumber = @event.TrackingNumber;
        UpdatedAt = @event.Timestamp;
    }

    #endregion

    // -------------------------------------------------------------------------
    // Snapshot support
    // -------------------------------------------------------------------------

    private record OrderSnapshotState(
        Guid Id,
        string OrderName,
        List<OrderItem> Items,
        string Tag,
        bool IsCancelled,
        string TrackingNumber,
        Guid CreatedById,
        DateTime UpdatedAt);

    public override bool SupportsSnapshots => true;

    public override string CreateSnapshot() =>
        JsonSerializer.Serialize(new OrderSnapshotState(
            Id, OrderName, new List<OrderItem>(Items),
            Tag, IsCancelled, TrackingNumber, CreatedById, UpdatedAt));

    protected override void RestoreFromSnapshot(string stateJson)
    {
        var s = JsonSerializer.Deserialize<OrderSnapshotState>(stateJson)
            ?? throw new InvalidOperationException("Failed to deserialize Order snapshot.");
        Id = s.Id;
        OrderName = s.OrderName;
        Items = s.Items.AsReadOnly();
        Tag = s.Tag;
        IsCancelled = s.IsCancelled;
        TrackingNumber = s.TrackingNumber;
        CreatedById = s.CreatedById;
        UpdatedAt = s.UpdatedAt;
    }
}