using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Domain;

/// <summary>
/// See Martin Fowler's definition on aggregates. In addition to that, our aggregates consist of events stream 
/// and don't have a state storage - they are constructed from list of events passed into constructor.
/// 
/// All modifications (mutations) to a state are made by triggering events.
/// 
/// All properties of an aggregate should have private setters.
/// 
/// See example in Order aggregate test case in CloudFabric.EventSourcing.EventStore.Tests\Domain\Order.cs
/// </summary>
/// <see cref="https://www.martinfowler.com/bliki/DDD_Aggregate.html"/>
/// <seealso cref="CloudFabric.EventSourcing.EventStore.Tests\Domain\Order.cs"/>
public abstract class AggregateBase
{
    public virtual Guid Id { get; protected set; }

    public AggregateBase()
    {
    }

    /// <summary>
    /// Use this constructor in aggregate creation constructors to set identity before applying events.
    /// <example>
    /// public Order(Guid id, string name) : base(id)
    /// {
    ///     Apply(new OrderPlaced(name));
    /// }
    /// </example>
    /// </summary>
    protected AggregateBase(Guid id)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Aggregate Id cannot be empty.", nameof(id));
        }

        Id = id;
    }

    public AggregateBase(IEnumerable<IEvent> events)
    {
        if (events == null)
        {
            throw new Exception("Aggregate should not be constructed with null events list");
        }

        var eventsList = events as IList<IEvent> ?? events.ToList();

        // Set identity from the first regular (non-cross-aggregate) event.
        // Cross-aggregate events have AggregateId = Guid.Empty and should not define the aggregate's identity.
        if (eventsList.Count > 0)
        {
            var firstRegularEvent = eventsList.FirstOrDefault(e => e is not ICrossAggregateEvent);
            if (firstRegularEvent != null)
            {
                Id = firstRegularEvent.AggregateId;
            }
        }

        foreach (var @event in eventsList)
        {
            if (@event == null)
            {
                throw new Exception("event is null");
            }

            RaiseEvent(@event);

            // Cross-aggregate events do not contribute to the aggregate's version.
            // Version is used for optimistic concurrency on the aggregate's own event stream.
            if (@event is not ICrossAggregateEvent)
            {
                Version += 1;
            }
        }
    }

    /// <summary>
    /// Aggregate's id is always a Guid.
    /// 
    /// That is not always handy when we need an aggregate with unique identifier as its id.
    /// Good example is UserEmailAddress domain aggregate. We need to be able to query database by email address string,
    /// but the only way to query an aggregate is by Guid.
    ///
    /// For such situations we would override UserEmailAddress.Id and make it return the value of HashStringToGuid(emailAddress).
    ///
    /// When querying we can simply create a new instance of an aggregate and use its id.
    ///
    /// </summary>
    /// <example>
    /// public override Guid Id
    /// {
    ///    get => HashStringToGuid(emailAddress)
    /// }
    /// </example>
    ///
    /// /// <example>
    /// var emailLookup = new UserEmailAddress("test@test.com");
    /// var existingEmailAddress = emailAddressRepository.Load(emailLookup.Id);
    /// </example>
    /// <param name="stringToHash"></param>
    /// <returns></returns>
    public static Guid HashStringToGuid(string stringToHash)
    {
        return DeterministicGuid.Create(stringToHash);
    }

    /// <summary>
    /// Number of events which happened to mutate this aggregate into it's current state.
    /// </summary>
    public int Version { get; internal set; }

    /// <summary>
    /// Changes - new events that were not stored to persistance yet.
    /// </summary>
    public List<IEvent> UncommittedEvents { get; protected set; } = new List<IEvent>();

    public void OnChangesSaved()
    {
        Version += UncommittedEvents.Count;
        UncommittedEvents.Clear();
    }

    public abstract string PartitionKey { get; }

    protected void Apply(IEvent @event)
    {
        if (Id == Guid.Empty)
        {
            throw new InvalidOperationException(
                $"Aggregate Id must be set before calling Apply(). " +
                $"Use the base(id) constructor: public {GetType().Name}(Guid id, ...) : base(id)"
            );
        }

        @event.AggregateId = Id;
        @event.PartitionKey = PartitionKey;

        RaiseEvent(@event);

        UncommittedEvents.Add(@event);
    }

    protected virtual void RaiseEvent(IEvent @event)
    {
        ((dynamic)this).On((dynamic)@event);
    }
}