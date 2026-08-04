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

            if (@event.Timestamp > LastAppliedEventTimestamp)
            {
                LastAppliedEventTimestamp = @event.Timestamp;
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

    // -------------------------------------------------------------------------
    // Snapshot support (opt-in)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Timestamp of the last event (own or cross-aggregate) applied during event replay.
    /// Set by the constructor when replaying events, and updated by <see cref="ApplyHistoricalEvent"/>.
    /// Used by <see cref="AggregateRepository{T}"/> to determine which cross-aggregate events
    /// have already been captured in a snapshot and therefore do not need to be replayed again.
    /// </summary>
    public DateTime LastAppliedEventTimestamp { get; private set; }

    /// <summary>
    /// Returns true if this aggregate supports snapshot serialization/deserialization.
    /// Override in derived classes and return true together with implementations of
    /// <see cref="CreateSnapshot"/> and <see cref="RestoreFromSnapshot"/>.
    /// </summary>
    public virtual bool SupportsSnapshots => false;

    /// <summary>
    /// Serializes the aggregate's current state to a JSON string for snapshot storage.
    /// Override in derived classes that support snapshots.
    /// </summary>
    public virtual string CreateSnapshot() =>
        throw new NotSupportedException(
            $"{GetType().Name} does not implement CreateSnapshot(). " +
            "Override SupportsSnapshots, CreateSnapshot, and RestoreFromSnapshot to enable snapshot support."
        );

    /// <summary>
    /// Restores the aggregate's state from a previously created snapshot JSON string.
    /// Override in derived classes that support snapshots. The override must restore all
    /// domain state including the aggregate <see cref="Id"/>.
    /// </summary>
    protected virtual void RestoreFromSnapshot(string stateJson) =>
        throw new NotSupportedException(
            $"{GetType().Name} does not implement RestoreFromSnapshot(). " +
            "Override SupportsSnapshots, CreateSnapshot, and RestoreFromSnapshot to enable snapshot support."
        );

    /// <summary>
    /// Called by <see cref="AggregateRepository{T}"/> to restore the aggregate from a snapshot.
    /// Initializes state, version, and the last-applied timestamp without replaying any events.
    /// </summary>
    internal void InitFromSnapshot(string stateJson, int version, DateTime lastAppliedEventTimestamp)
    {
        RestoreFromSnapshot(stateJson);
        Version = version;
        LastAppliedEventTimestamp = lastAppliedEventTimestamp;
    }

    /// <summary>
    /// Called by <see cref="AggregateRepository{T}"/> to replay a single historical event on an
    /// aggregate that was already initialized from a snapshot. Updates <see cref="Version"/> and
    /// <see cref="LastAppliedEventTimestamp"/> accordingly.
    /// </summary>
    internal void ApplyHistoricalEvent(IEvent @event)
    {
        // If Id was not set by snapshot restoration (e.g. blank aggregate), derive it from the first own event.
        if (@event is not ICrossAggregateEvent && Id == Guid.Empty)
        {
            Id = @event.AggregateId;
        }

        RaiseEvent(@event);

        if (@event is not ICrossAggregateEvent)
        {
            Version++;
        }

        if (@event.Timestamp > LastAppliedEventTimestamp)
        {
            LastAppliedEventTimestamp = @event.Timestamp;
        }
    }
}