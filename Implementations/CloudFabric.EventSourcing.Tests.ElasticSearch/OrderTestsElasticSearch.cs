using System.Diagnostics;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using CloudFabric.Projections;
using CloudFabric.Projections.ElasticSearch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.ElasticSearch;

/// <summary>
/// Elastic Search projections test with Postgresql event store
/// </summary>
[TestClass]
public class OrderTestsElasticSearch : OrderTests
{
    private ProjectionRepositoryFactory? _projectionRepositoryFactory;
    private PostgresqlEventStore? _eventStore;
    private PostgresqlEventStoreEventObserver? _eventStoreEventsObserver;

    protected override async Task<IEventStore> GetEventStore()
    {
        if (_eventStore == null)
        {
            _eventStore = new PostgresqlEventStore(
                TestsConfiguration.PostgresConnectionStringForDatabase("cloudfabric_es_test_es"),
                "orders_events",
                "stored_items"
            );
            await _eventStore.Initialize();
        }

        return _eventStore;
    }

    protected override ProjectionRepositoryFactory GetProjectionRepositoryFactory()
    {
        if (_projectionRepositoryFactory == null)
        {
            _projectionRepositoryFactory = new ElasticSearchProjectionRepositoryFactory(
                new ElasticSearchBasicAuthConnectionSettings(
                TestsConfiguration.ElasticsearchUrl,
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

    public async Task LoadTest()
    {
        var watch = Stopwatch.StartNew();

        var tasks = new List<Task>();

        for (var i = 0; i < 100; i++)
        {
            for (var j = 0; j < 10; j++)
            {
                tasks.Add(TestPlaceOrderAndAddItem());
            }
        }

        await Task.WhenAll(tasks);

        watch.Stop();

        Console.WriteLine($"It took {watch.Elapsed}!");
    }
}