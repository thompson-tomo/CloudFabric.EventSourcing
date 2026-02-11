using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;
using CloudFabric.EventSourcing.Tests.Domain.Projections.OrdersListProjection;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;
using CloudFabric.Projections;
using CloudFabric.Projections.Queries;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests;

public abstract class ProjectionQueryTest : TestsBaseWithProjections<OrderListProjectionItem, OrdersListProjectionBuilder>
{
    [TestInitialize]
    public new async Task Initialize()
    {
        await base.Initialize();
    }

    [TestMethod]
    public async Task TestProjectionQuerySerializationDeserialization()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());

        // Repository containing projections - `view models` of orders
        var ordersListProjectionsRepository = GetProjectionRepositoryFactory().GetProjectionRepository<OrderListProjectionItem>();
        var orderRepositoryEventsObserver = GetEventStoreEventsObserver();

        // Projections engine - takes events from events observer and passes them to multiple projection builders
        var projectionsEngine = new ProjectionsEngine(orderRepositoryEventsObserver);

        var ordersListProjectionBuilder = new OrdersListProjectionBuilder(GetProjectionRepositoryFactory());
        projectionsEngine.AddProjectionBuilder(ordersListProjectionBuilder);

        string instanceName = "ProjectionsQueryInstance";

        await projectionsEngine.StartAsync(instanceName);


        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                100.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                200.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                300.00m
            )
        };

        var firstOrder = new Order(Guid.NewGuid(), "First queryable order", items, userId, null);
        await orderRepository.SaveOrder(userInfo, firstOrder);

        // second order will contain only one item
        var secondOrder = new Order(Guid.NewGuid(), "Second queryable order with additional parameter", items.GetRange(0, 1), userId, null);
        await orderRepository.SaveOrder(userInfo, secondOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        var query = new ProjectionQuery
        {
            SearchText = "ORDER",
            Limit = 1,
            OrderBy = new List<SortInfo>()
            {
                new SortInfo()
                {
                    KeyPath = "ItemsCount",
                    Order = "desc"
                }
            },
            Filters = new List<Filter>()
            {
                new Filter()
                {
                    // just for example purposes, this is how you combine two filters with  OR - just wrap them in empty filter
                    // SQL presentation of this filter will look like:
                    // (ItemsCount > 1 AND UpdatedAt > 23.02.2023 19:06:16) OR (ItemsCount = 1 AND UpdatedAt > 23.02.2023 19:06:16)
                    Filters = new List<FilterConnector>()
                    {
                        new FilterConnector(FilterLogic.Or, new Filter("ItemsCount", FilterOperator.Greater, (long)1)
                            .And("UpdatedAt", FilterOperator.GreaterOrEqual, DateTime.UtcNow.AddMinutes(-1))),
                        new FilterConnector(FilterLogic.Or, new Filter("ItemsCount", FilterOperator.Equal, (long)1)
                            .And("UpdatedAt", FilterOperator.GreaterOrEqual, DateTime.UtcNow.AddMinutes(-1)))
                    }
                }
            }
        };

        // "sv1_*|*||True||or+ItemsCount|gt|1|True||and+UpdatedAt|ge|23%3Bdot%3B02%3Bdot%3B2023+19%3A05%3A37|True||.or+ItemsCount|eq|1|True||and+UpdatedAt|ge|23%3Bdot%3B02%3Bdot%3B2023+19%3A05%3A37|True||"
        var queryFiltersSerialized = query.SerializeFiltersToQueryString();
        
        var orderBySerialized = query.SerializeOrderByToQueryString();

        var queryDeserialized = new ProjectionQuery()
        {
            Limit = query.Limit,
            Offset = query.Offset,
            SearchText = query.SearchText
        };
        queryDeserialized.DeserializeFiltersQueryString(queryFiltersSerialized);
        queryDeserialized.DeserializeOrderByQueryString(orderBySerialized);
        
        // query by name
        var orders = await ordersListProjectionsRepository.Query(query);
        orders.TotalRecordsFound.Should().Be(2);
        orders.Records.Count.Should().Be(1);

        var ordersFromSerializedQuery = await ordersListProjectionsRepository.Query(queryDeserialized);
        ordersFromSerializedQuery.TotalRecordsFound.Should().Be(2);
        ordersFromSerializedQuery.Records.Count.Should().Be(1);

        orders.Records.Should().BeEquivalentTo(ordersFromSerializedQuery.Records);

        await projectionsEngine.StopAsync();
    }
}
