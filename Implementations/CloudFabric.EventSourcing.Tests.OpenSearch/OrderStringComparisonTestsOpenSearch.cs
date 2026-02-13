using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using CloudFabric.Projections;
using CloudFabric.Projections.OpenSearch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.OpenSearch;

/// <summary>
/// OpenSearch projections test with Postgresql event store
/// </summary>
[TestClass]
public class OrderStringComparisonTestsOpenSearch : OrderStringComparisonTests
{
    private ProjectionRepositoryFactory? _projectionRepositoryFactory;
    private PostgresqlEventStore? _eventStore;
    private PostgresqlEventStoreEventObserver? _eventStoreEventsObserver;

    protected override async Task<IEventStore> GetEventStore()
    {
        if (_eventStore == null)
        {
            _eventStore = new PostgresqlEventStore(
                TestsConfiguration.PostgresConnectionStringForDatabase("cloudfabric_es_test_os"),
                "orders_events",
                "orders_items"
            );
            await _eventStore.Initialize();
        }

        return _eventStore;
    }

    protected override ProjectionRepositoryFactory GetProjectionRepositoryFactory()
    {
        if (_projectionRepositoryFactory == null)
        {
            _projectionRepositoryFactory = new OpenSearchProjectionRepositoryFactory(
                new OpenSearchBasicAuthConnectionSettings(
                TestsConfiguration.OpenSearchUrl,
                "",
                "",
                ""),
                new LoggerFactory(),
                true
            );
        }

        return _projectionRepositoryFactory;
    }

    protected override EventsObserver GetEventStoreEventsObserver()
    {
        if (_eventStoreEventsObserver == null)
        {
            _eventStoreEventsObserver = new PostgresqlEventStoreEventObserver(
                _eventStore,
                NullLogger<PostgresqlEventStoreEventObserver>.Instance
            );
        }

        return _eventStoreEventsObserver;
    }
}
