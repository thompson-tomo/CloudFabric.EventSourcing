using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Domain;

public class AggregateRepositoryFactory
{
    private readonly IEventStore _eventStore;
    
    public AggregateRepositoryFactory(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public AggregateRepository<TAggregate> GetAggregateRepository<TAggregate>() where TAggregate : AggregateBase
    {
        return new AggregateRepository<TAggregate>(_eventStore);
    }
}
