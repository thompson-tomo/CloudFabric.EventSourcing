using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;
using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.EventSourcing.Tests.Domain.Projections.OrdersListProjection;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;
using CloudFabric.Projections;
using CloudFabric.Projections.Queries;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests;

public abstract class OrderTests : TestsBaseWithProjections<OrderListProjectionItem, OrdersListProjectionBuilder>
{
    [TestInitialize]
    public new async Task Initialize()
    {
        await base.Initialize();
    }
    
    [TestMethod]
    public async Task TestPlaceOrder()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var id = Guid.NewGuid();
        var orderName = "Birthday Gift";
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "Caverna",
                12.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Dixit",
                6.59m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Patchwork",
                4.85m
            )
        };
        var order = new Order(id, orderName, items, userId, "john@gmail.com");

        await orderRepository.SaveAsync(userInfo, order);
        var order2 = await orderRepository.LoadAsync(id, PartitionKeys.GetOrderPartitionKey());
        order2.Id.Should().Be(id);
        order2.OrderName.Should().Be(orderName);
        order2.Items.Should().BeEquivalentTo(items);
        order2.Items.Count.Should().Be(3);
    }


    [TestMethod]
    public async Task TestOrderNotFound()
    {
        var orderRepository = new OrderRepository(await GetEventStore());

        var act = async () => await orderRepository.LoadOrder(Guid.Empty, PartitionKeys.GetOrderPartitionKey());

        await act.Should().ThrowAsync<NotFoundException>();
    }

    [TestMethod]
    public async Task TestPlaceOrderAndAddItem()
    {
        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var id = Guid.NewGuid();
        var orderName = "Birthday Gift";
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "Caverna",
                12.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Dixit",
                6.59m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Patchwork",
                4.85m
            )
        };

        var order = new Order(id, orderName, items, userId, "john@gmail.com");
        var orderRepository = new OrderRepository(await GetEventStore());

        // add another item:
        var addItem = new OrderItem(DateTime.UtcNow, "Eclipse", 6.95m);
        order.AddItem(addItem);
        await orderRepository.SaveOrder(userInfo, order);

        // update items so we can use it for comparison
        items.Add(addItem);

        var order2 = await orderRepository.LoadOrder(id, PartitionKeys.GetOrderPartitionKey());
        order2.Id.Should().Be(id);
        order2.OrderName.Should().Be(orderName);
        order2.Items.Should().BeEquivalentTo(items);
        order2.Items.Count.Should().Be(4);


        // add few other events
        for (var i = 0; i < 100; i++)
        {
            var addItemLoop = new OrderItem(DateTime.UtcNow, $"Eclipse-{i}", 6.95m + i);
            order2.AddItem(addItemLoop);
            items.Add(addItemLoop);
        }

        await orderRepository.SaveOrder(userInfo, order2);

        var order3 = await orderRepository.LoadOrder(id, PartitionKeys.GetOrderPartitionKey());
        order3.Id.Should().Be(id);
        order3.OrderName.Should().Be(orderName);
        order3.Items.Should().BeEquivalentTo(items);
        order3.Items.Count.Should().Be(104);
    }


    [TestMethod]
    public async Task TestPlaceOrderAndAddItemProjections()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var id = Guid.NewGuid();
        var orderName = "New Year's Gifts";
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "Colonizing Mars",
                12.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Dixit",
                6.59m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Time Stories",
                4.85m
            )
        };

        var order = new Order(id, orderName, items, userId, "john@gmail.com");

        await orderRepository.SaveOrder(userInfo, order);

        await Task.Delay(ProjectionsUpdateDelay);

        var orderProjection = await ProjectionsRepository.Single(id, PartitionKeys.GetOrderPartitionKey());
        orderProjection.Should().NotBeNull();

        orderProjection.Name.Should().Be(orderName);
        orderProjection.ItemsCount.Should().Be(items.Count);
        orderProjection.Items.Count.Should().Be(items.Count);
        orderProjection.CreatedBy.UserId.Should().Be(userId);

        var addItem = new OrderItem(DateTime.UtcNow, "Twilight Struggle", 6.95m);
        order.AddItem(addItem);
        order.AddItem(addItem);
        order.AddItem(addItem);
        order.AddItem(addItem);

        await orderRepository.SaveOrder(userInfo, order);

        await Task.Delay(ProjectionsUpdateDelay);

        items.Add(addItem);
        items.Add(addItem);
        items.Add(addItem);
        items.Add(addItem);

        var order2 = await orderRepository.LoadOrder(id, PartitionKeys.GetOrderPartitionKey());
        order2.Id.Should().Be(id);
        order2.OrderName.Should().Be(orderName);
        order2.Items.Should().BeEquivalentTo(items);
        order2.Items.Count.Should().Be(7);

        var orderProjection2 = await ProjectionsRepository.Single(id, PartitionKeys.GetOrderPartitionKey());
        orderProjection2.Should().NotBeNull();

        orderProjection2.Name.Should().Be(orderName);
        orderProjection2.ItemsCount.Should().Be(7);
        orderProjection2.CreatedBy.UserId.Should().Be(userId);

        var orderProjectionFromQuery =
            await ProjectionsRepository.Query(
                ProjectionQueryExpressionExtensions.Where<OrderListProjectionItem>(d => d.Name == orderName)
            );
        orderProjectionFromQuery.Records.Count.Should().Be(1);
        orderProjectionFromQuery.Records.First().Document!.Name.Should().Be(orderName);
    }

    [TestMethod]
    public async Task TestRebuildOrderDocumentProjection()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "RebuildDocumentItem",
                12.00m
            )
        };

        var firstOrder = new Order(Guid.NewGuid(), "Rebuild product first order", items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, firstOrder);

        var secondOrder = new Order(Guid.NewGuid(), "Rebuild product second order", items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, secondOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        var firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        var secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection.Should().NotBeNull();
        secondOrderProjection.Should().NotBeNull();

        // remove orders
        await ProjectionsRepository.Delete(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        await ProjectionsRepository.Delete(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());
        firstOrderProjection.Should().BeNull();
        secondOrderProjection.Should().BeNull();

        // rebuild the firstOrder document
        await ProjectionsEngine.RebuildOneAsync(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());

        // check firstOrder document is rebuild and second is not
        firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection.Should().NotBeNull();
        secondOrderProjection.Should().BeNull();
    }

    [TestMethod]
    public async Task TestRebuildAllOrdersProjections()
    {
        string instanceName = "RebuildOrdersTestInstance";

        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "RebuildDocumentItem",
                12.00m
            )
        };

        var firstOrder = new Order(Guid.NewGuid(), "Rebuild orders first order", items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, firstOrder);

        var secondOrder = new Order(Guid.NewGuid(), "Rebuild orders second order", items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, secondOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        var firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        var secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection.Should().NotBeNull();
        secondOrderProjection.Should().NotBeNull();

        // remove orders
        await ProjectionsRepository.Delete(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        await ProjectionsRepository.Delete(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());
        firstOrderProjection.Should().BeNull();
        secondOrderProjection.Should().BeNull();

        await ProjectionsRepository.DeleteAll();
        await ProjectionsRepository.EnsureIndex();
        await ProjectionsRebuildProcessor.RebuildProjectionsThatRequireRebuild();

        // check firstOrder document is rebuild and second is not
        firstOrderProjection = await ProjectionsRepository.Single(firstOrder.Id, PartitionKeys.GetOrderPartitionKey());
        secondOrderProjection = await ProjectionsRepository.Single(secondOrder.Id, PartitionKeys.GetOrderPartitionKey());

        firstOrderProjection.Should().NotBeNull();
        secondOrderProjection.Should().NotBeNull();
    }

    [TestMethod]
    public async Task TestProjectionsQuery()
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
                111.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                111.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                111.00m
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
            Limit = 1
        };

        // query by name
        var orders = await ordersListProjectionsRepository.Query(query);
        orders.TotalRecordsFound.Should().Be(2);
        orders.Records.Count.Should().Be(1);

        query.SearchText = "queryable order";
        query.Limit = null;

        orders = await ordersListProjectionsRepository.Query(query);
        await Task.Delay(ProjectionsUpdateDelay);
        orders.TotalRecordsFound.Should().Be(2);
        orders.Records.Count.Should().Be(2);

        // add filter by count
        orders = await ordersListProjectionsRepository.Query(
            ProjectionQueryExpressionExtensions.Where<OrderListProjectionItem>(x => x.ItemsCount > 1)
        );
        orders.TotalRecordsFound.Should().Be(1);
        orders.Records.Count.Should().Be(1);

        await projectionsEngine.StopAsync();
    }

    [TestMethod]
    public virtual async Task TestProjectionsNestedObjectsQuery()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());
        var ordersListProjectionsRepository = GetProjectionRepositoryFactory().GetProjectionRepository<OrderListProjectionItem>();
        
        var userInfo1 = new EventUserInfo(Guid.NewGuid());
        var userInfo2 = new EventUserInfo(Guid.NewGuid());
        var userInfo3 = new EventUserInfo(Guid.NewGuid());
        var firstOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Colonizing Mars", 12.00m),
            new OrderItem(DateTime.UtcNow, "Patchwork", 6.59m),
            new OrderItem(DateTime.UtcNow, "Time Stories", 4.85m)
        };

        var firstOrder = new Order(Guid.NewGuid(), "New Years Gifts", firstOrderItems, userInfo1.UserId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo1, firstOrder);

        var secondOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Caverna", 12.00m),
            new OrderItem(DateTime.UtcNow, "Dixit", 6.59m)
        };

        var secondOrder = new Order(Guid.NewGuid(), "Birthday Gifts", secondOrderItems, userInfo2.UserId, "will@gmail.com");
        await orderRepository.SaveOrder(userInfo2, secondOrder);

        var thirdOrder = new Order(Guid.NewGuid(), "Christmas Gifts", new List<OrderItem>(), userInfo3.UserId, "amy@gmail.com");
        await orderRepository.SaveOrder(userInfo3, thirdOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        // search by nested Items array
        var query = new ProjectionQuery
        {
            SearchText = "stories tim"
        };

        // query by name
        var orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(1);
        orders.Records.First().Document!.Items.Count.Should().Be(3);

        query.SearchText = "dixit";
        orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(1);
        orders.Records.First().Document!.Items.Count.Should().Be(2);

        query.SearchText = "amy@gmail.co";
        orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(1);
        orders.Records.First().Document!.Items.Count.Should().Be(0);
    }

    [TestMethod]
    public virtual async Task TestProjectionsNestedObjectsFilter()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());
        var ordersListProjectionsRepository = GetProjectionRepositoryFactory().GetProjectionRepository<OrderListProjectionItem>();

        var userInfo1 = new EventUserInfo(Guid.NewGuid());
        var userInfo2 = new EventUserInfo(Guid.NewGuid());
        var userInfo3 = new EventUserInfo(Guid.NewGuid());
        var firstOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Colonizing Mars", 12.00m),
            new OrderItem(DateTime.UtcNow.AddDays(-7), "Patchwork", 6.59m),
            new OrderItem(DateTime.UtcNow, "Time Stories", 4.85m)
        };

        var firstOrder = new Order(Guid.NewGuid(), "New Years Gifts", firstOrderItems, userInfo1.UserId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo1, firstOrder);

        var secondOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Caverna", 12.00m),
            new OrderItem(DateTime.UtcNow, "Dixit", 6.59m)
        };

        var secondOrder = new Order(Guid.NewGuid(), "Birthday Gifts", secondOrderItems, userInfo2.UserId, "will@gmail.com");
        await orderRepository.SaveOrder(userInfo2, secondOrder);

        var thirdOrder = new Order(Guid.NewGuid(), "Christmas Gifts", new List<OrderItem>(), userInfo3.UserId, "amy@gmail.com");
        await orderRepository.SaveOrder(userInfo3, thirdOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        // filter by creator id
        var query = new ProjectionQuery();
        query.Filters.Add(
            new Filter
            {
                PropertyName = "CreatedBy.UserId",
                Operator = FilterOperator.Equal,
                Value = userInfo2.UserId
            }
        );

        var orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(1);
        orders.Records.First().Document!.CreatedBy.UserId.Should().Be(userInfo2.UserId);

        // filter by items date
        query.Filters[0] = new Filter
        {
            PropertyName = "Items.AddedAt",
            Operator = FilterOperator.Lower,
            Value = DateTime.UtcNow.AddDays(-1)
        };

        orders = await ProjectionsRepository.Query(query);

        orders.Records.Count.Should().Be(1);
        orders.Records.First().Document!.Items.Count.Should().Be(3);

        // filter by items Amount
        query.Filters[0] = new Filter
        {
            PropertyName = "Items.Amount",
            Operator = FilterOperator.GreaterOrEqual,
            Value = 5m
        };

        orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(2);
        orders.Records.Any(x => x.Document!.Items.Count == 3).Should().BeTrue();
        orders.Records.Any(x => x.Document!.Items.Count == 2).Should().BeTrue();
    }
    
    
    [TestMethod]
    public virtual async Task TestProjectionsNestedObjectsSorting()
    {
        // Event sourced repository storing streams of events. Main source of truth for orders.
        var orderRepository = new OrderRepository(await GetEventStore());

        // Repository containing projections - `view models` of orders
        var ordersListProjectionsRepository = GetProjectionRepositoryFactory().GetProjectionRepository<OrderListProjectionItem>();

        var userInfo1 = new EventUserInfo(Guid.NewGuid());
        var userInfo2 = new EventUserInfo(Guid.NewGuid());
        var userInfo3 = new EventUserInfo(Guid.NewGuid());
        var firstOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Colonizing Mars", 12.00m),
            new OrderItem(DateTime.UtcNow, "Patchwork", 999m),
            new OrderItem(DateTime.UtcNow, "Time Stories", 4.85m)
        };

        var firstOrder = new Order(Guid.NewGuid(), "New Years Gifts", firstOrderItems, userInfo1.UserId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo1, firstOrder);

        var secondOrderItems = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Caverna", 999m),
            new OrderItem(DateTime.UtcNow, "Dixit", 6.59m)
        };

        var secondOrder = new Order(Guid.NewGuid(), "Birthday Gifts", secondOrderItems, userInfo2.UserId, "will@gmail.com");
        await orderRepository.SaveOrder(userInfo2, secondOrder);

        var thirdOrder = new Order(Guid.NewGuid(), "Christmas Gifts", new List<OrderItem>(), userInfo3.UserId, "amy@gmail.com");
        await orderRepository.SaveOrder(userInfo3, thirdOrder);

        await Task.Delay(ProjectionsUpdateDelay);

        // search by nested Items array
        var query = new ProjectionQuery
        {
            OrderBy = new List<SortInfo>
            {
                new SortInfo
                {
                    KeyPath = "CreatedBy.Email",
                    Order = "desc"
                }
            }
        };

        // query by name
        var orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.Count.Should().Be(3);
        orders.Records.ElementAt(0).Document!.Id.Should().Be(secondOrder.Id);
        orders.Records.ElementAt(1).Document!.Id.Should().Be(firstOrder.Id);
        orders.Records.ElementAt(2).Document!.Id.Should().Be(thirdOrder.Id);

        // test sorting by array value with filter
        query.OrderBy = new List<SortInfo>
        {
            new SortInfo
            {
                KeyPath = "Items.Name",
                Order = "asc",
                Filters = new List<SortingFilter>
                {
                    new SortingFilter
                    {
                        FilterKeyPath = "Items.Amount",
                        FilterValue = 999m
                    }
                }
            }
        };
        
        orders = await ordersListProjectionsRepository.Query(query);
        orders.Records.ElementAt(0).Document!.Id.Should().Be(secondOrder.Id);
        orders.Records.ElementAt(1).Document!.Id.Should().Be(firstOrder.Id);
        orders.Records.ElementAt(2).Document!.Id.Should().Be(thirdOrder.Id);
    }

    [TestMethod]
    public virtual async Task TestProjectionDocumentUpdatedAt()
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

        var order = new Order(Guid.NewGuid(), "First test order", new List<OrderItem>(), userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order);

        await Task.Delay(ProjectionsUpdateDelay);

        var orders = await ordersListProjectionsRepository.Query(new ProjectionQuery());
        orders.TotalRecordsFound.Should().Be(1);
        orders.Records.First().Document!.UpdatedAt.ToString().Should().Be(order.UpdatedAt.ToString());

        order.AddItem(
            new OrderItem(
                DateTime.UtcNow,
                "Test",
                111.00m
            )
        );

        await orderRepository.SaveOrder(userInfo, order);

        await Task.Delay(ProjectionsUpdateDelay);

        orders = await ordersListProjectionsRepository.Query(new ProjectionQuery());
        orders.TotalRecordsFound.Should().Be(1);
        orders.Records.First().Document!.UpdatedAt.ToString().Should().Be(order.UpdatedAt.ToString());

        await projectionsEngine.StopAsync();
    }

    [TestMethod]
    public async Task TestHardDeleteOrder()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var id = Guid.NewGuid();
        var orderName = "Birthday Gift";
        var items = new List<OrderItem>
        {
            new OrderItem(
                DateTime.UtcNow,
                "Caverna",
                12.00m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Dixit",
                6.59m
            ),
            new OrderItem(
                DateTime.UtcNow,
                "Patchwork",
                4.85m
            )
        };
        var order = new Order(id, orderName, items, userId, "john@gmail.com");

        await orderRepository.SaveAsync(userInfo, order);

        await orderRepository.HardDeleteAsync(order.Id, order.PartitionKey);

        var order2 = await orderRepository.LoadAsync(id, PartitionKeys.GetOrderPartitionKey());
        order2.Should().BeNull();
    }

    #region Cross-Aggregate Event Tests

    [TestMethod]
    public virtual async Task TestCrossAggregateEvent_AggregateSeesGlobalEvent()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new AggregateRepository<Order>(eventStore);

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create two orders
        var order1 = new Order(Guid.NewGuid(), "Order One", new List<OrderItem>(), userId, "john@gmail.com");
        var order2 = new Order(Guid.NewGuid(), "Order Two", new List<OrderItem>(), userId, "jane@gmail.com");

        await orderRepository.SaveAsync(userInfo, order1);
        await orderRepository.SaveAsync(userInfo, order2);

        // Verify orders have no tag initially
        var loaded1 = await orderRepository.LoadAsync(order1.Id, PartitionKeys.GetOrderPartitionKey());
        loaded1.Should().NotBeNull();
        loaded1!.Tag.Should().BeEmpty();
        var versionBefore = loaded1.Version;

        // Append a cross-aggregate event
        var bulkEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "SALE",
            PartitionKeys.GetOrderPartitionKey()
        );
        await eventStore.AppendGlobalEventAsync(userInfo, bulkEvent);

        // Load orders again — they should see the global event
        var reloaded1 = await orderRepository.LoadAsync(order1.Id, PartitionKeys.GetOrderPartitionKey());
        var reloaded2 = await orderRepository.LoadAsync(order2.Id, PartitionKeys.GetOrderPartitionKey());

        reloaded1.Should().NotBeNull();
        reloaded2.Should().NotBeNull();

        reloaded1!.Tag.Should().Be("SALE");
        reloaded2!.Tag.Should().Be("SALE");

        // Version should NOT have changed (cross-aggregate events don't count)
        reloaded1.Version.Should().Be(versionBefore);
        reloaded2.Version.Should().Be(versionBefore);
    }

    [TestMethod]
    public virtual async Task TestCrossAggregateEvent_OptimisticConcurrencyNotAffected()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new AggregateRepository<Order>(eventStore);

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create an order
        var order = new Order(Guid.NewGuid(), "Concurrency Test Order", new List<OrderItem>(), userId, "john@gmail.com");
        await orderRepository.SaveAsync(userInfo, order);

        // Append a cross-aggregate event
        var bulkEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "UPDATED",
            PartitionKeys.GetOrderPartitionKey()
        );
        await eventStore.AppendGlobalEventAsync(userInfo, bulkEvent);

        // Load the order (it will see the global event) and make a modification
        var loaded = await orderRepository.LoadAsync(order.Id, PartitionKeys.GetOrderPartitionKey());
        loaded.Should().NotBeNull();
        loaded!.Tag.Should().Be("UPDATED");

        // Saving a new regular event should succeed — version unchanged by cross-aggregate event
        loaded.AddItem(new OrderItem(DateTime.UtcNow, "NewItem", 10.00m));
        var saveResult = await orderRepository.SaveAsync(userInfo, loaded);
        saveResult.Should().BeTrue();

        // Reload and verify both changes are visible
        var final = await orderRepository.LoadAsync(order.Id, PartitionKeys.GetOrderPartitionKey());
        final.Should().NotBeNull();
        final!.Tag.Should().Be("UPDATED");
        final.Items.Count.Should().Be(1);
        final.Items[0].Name.Should().Be("NewItem");
    }

    [TestMethod]
    public virtual async Task TestCrossAggregateEvent_ProjectionBulkUpdate()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new OrderRepository(eventStore);

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create two orders
        var order1 = new Order(Guid.NewGuid(), "Projection Bulk Order 1", new List<OrderItem>(), userId, "john@gmail.com");
        var order2 = new Order(Guid.NewGuid(), "Projection Bulk Order 2", new List<OrderItem>(), userId, "jane@gmail.com");

        await orderRepository.SaveOrder(userInfo, order1);
        await orderRepository.SaveOrder(userInfo, order2);

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify initial state — no tag
        var proj1 = await ProjectionsRepository.Single(order1.Id, PartitionKeys.GetOrderPartitionKey());
        var proj2 = await ProjectionsRepository.Single(order2.Id, PartitionKeys.GetOrderPartitionKey());
        proj1.Should().NotBeNull();
        proj2.Should().NotBeNull();
        proj1!.Tag.Should().BeEmpty();
        proj2!.Tag.Should().BeEmpty();

        // Append a cross-aggregate event (this goes through event store observers → ProjectionsEngine)
        var bulkEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "BULK_UPDATED",
            PartitionKeys.GetOrderPartitionKey()
        );
        await eventStore.AppendGlobalEventAsync(userInfo, bulkEvent);

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify projections were updated via UpdateByQuery
        var updatedProj1 = await ProjectionsRepository.Single(order1.Id, PartitionKeys.GetOrderPartitionKey());
        var updatedProj2 = await ProjectionsRepository.Single(order2.Id, PartitionKeys.GetOrderPartitionKey());

        updatedProj1.Should().NotBeNull();
        updatedProj2.Should().NotBeNull();
        updatedProj1!.Tag.Should().Be("BULK_UPDATED");
        updatedProj2!.Tag.Should().Be("BULK_UPDATED");

        // Names should remain unchanged
        updatedProj1.Name.Should().Be("Projection Bulk Order 1");
        updatedProj2.Name.Should().Be("Projection Bulk Order 2");
    }

    [TestMethod]
    public virtual async Task TestCrossAggregateEvent_RebuildIncludesGlobalEvents()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new OrderRepository(eventStore);

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create an order
        var order = new Order(Guid.NewGuid(), "Rebuild Cross-Agg Order", new List<OrderItem>(), userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order);

        // Append a cross-aggregate event
        var bulkEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "REBUILT_TAG",
            PartitionKeys.GetOrderPartitionKey()
        );
        await eventStore.AppendGlobalEventAsync(userInfo, bulkEvent);

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify initial projection update worked
        var proj = await ProjectionsRepository.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull();
        proj!.Tag.Should().Be("REBUILT_TAG");

        // Delete projections and rebuild
        await ProjectionsRepository.DeleteAll();
        await ProjectionsRepository.EnsureIndex();
        await ProjectionsRebuildProcessor.RebuildProjectionsThatRequireRebuild();

        // After rebuild, the cross-aggregate event should be replayed
        var rebuiltProj = await ProjectionsRepository.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        rebuiltProj.Should().NotBeNull();
        rebuiltProj!.Tag.Should().Be("REBUILT_TAG");
        rebuiltProj.Name.Should().Be("Rebuild Cross-Agg Order");
    }

    #endregion
}