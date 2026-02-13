using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using CloudFabric.Projections;
using CloudFabric.Projections.InMemory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.InMemory;

[TestClass]
public class OrderTestsInMemory : OrderTests
{
    private ProjectionRepositoryFactory? _projectionRepositoryFactory;
    private InMemoryEventStore? _eventStore = null;
    private InMemoryEventStoreEventObserver? _eventStoreEventsObserver = null;

    protected override async Task<IEventStore> GetEventStore()
    {
        if (_eventStore == null)
        {
            _eventStore = new InMemoryEventStore(
                new System.Collections.Concurrent.ConcurrentDictionary<(Guid, string), List<string>>()
            );
            await _eventStore.Initialize();
        }

        return _eventStore;
    }

    protected override EventsObserver GetEventStoreEventsObserver()
    {
        if (_eventStoreEventsObserver == null)
        {
            _eventStoreEventsObserver = new InMemoryEventStoreEventObserver(_eventStore, NullLogger<InMemoryEventStoreEventObserver>.Instance);
        }

        return _eventStoreEventsObserver;
    }

    protected override ProjectionRepositoryFactory GetProjectionRepositoryFactory()
    {
        if (_projectionRepositoryFactory == null)
        {
            _projectionRepositoryFactory = new InMemoryProjectionRepositoryFactory(NullLoggerFactory.Instance);
        }

        return _projectionRepositoryFactory;
    }

}