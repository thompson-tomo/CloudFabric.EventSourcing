using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;
using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.EventSourcing.Tests.Domain.Projections.OrdersListProjection;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;
using CloudFabric.Projections;
using CloudFabric.Projections.Queries;
using CloudFabric.Projections.Worker;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
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

    [TestMethod]
    public virtual async Task TestProjectionsQueryFilterDateTimeGreaterVsGreaterOrEqual()
    {
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Use a fixed boundary date for precision
        var boundaryDate = new DateTime(2020, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        // Order 1: item exactly at boundary
        var order1Items = new List<OrderItem>
        {
            new OrderItem(boundaryDate, "Boundary Item", 10.00m)
        };
        var order1 = new Order(Guid.NewGuid(), "Boundary Order", order1Items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order1);

        // Order 2: item after boundary
        var order2Items = new List<OrderItem>
        {
            new OrderItem(boundaryDate.AddHours(1), "After Item", 20.00m)
        };
        var order2 = new Order(Guid.NewGuid(), "After Order", order2Items, userId, "jane@gmail.com");
        await orderRepository.SaveOrder(userInfo, order2);

        await Task.Delay(ProjectionsUpdateDelay);

        // GreaterOrEqual should include the boundary (both orders have items >= boundaryDate)
        var queryGte = new ProjectionQuery();
        queryGte.Filters.Add(new Filter
        {
            PropertyName = "Items.AddedAt",
            Operator = FilterOperator.GreaterOrEqual,
            Value = boundaryDate
        });

        var ordersGte = await ProjectionsRepository.Query(queryGte);
        ordersGte.TotalRecordsFound.Should().Be(2, "GreaterOrEqual should include the boundary value");

        // Greater should exclude the boundary (only order2 has item > boundaryDate)
        var queryGt = new ProjectionQuery();
        queryGt.Filters.Add(new Filter
        {
            PropertyName = "Items.AddedAt",
            Operator = FilterOperator.Greater,
            Value = boundaryDate
        });

        var ordersGt = await ProjectionsRepository.Query(queryGt);
        ordersGt.TotalRecordsFound.Should().Be(1, "Greater should exclude the boundary value");
    }

    [TestMethod]
    public virtual async Task TestProjectionsQueryFilterDateTimeLowerVsLowerOrEqual()
    {
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        var boundaryDate = new DateTime(2020, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        // Order 1: item exactly at boundary
        var order1Items = new List<OrderItem>
        {
            new OrderItem(boundaryDate, "Boundary Item", 10.00m)
        };
        var order1 = new Order(Guid.NewGuid(), "Boundary Order", order1Items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order1);

        // Order 2: item before boundary
        var order2Items = new List<OrderItem>
        {
            new OrderItem(boundaryDate.AddHours(-1), "Before Item", 20.00m)
        };
        var order2 = new Order(Guid.NewGuid(), "Before Order", order2Items, userId, "jane@gmail.com");
        await orderRepository.SaveOrder(userInfo, order2);

        await Task.Delay(ProjectionsUpdateDelay);

        // LowerOrEqual should include the boundary (both orders have items <= boundaryDate)
        var queryLte = new ProjectionQuery();
        queryLte.Filters.Add(new Filter
        {
            PropertyName = "Items.AddedAt",
            Operator = FilterOperator.LowerOrEqual,
            Value = boundaryDate
        });

        var ordersLte = await ProjectionsRepository.Query(queryLte);
        ordersLte.TotalRecordsFound.Should().Be(2, "LowerOrEqual should include the boundary value");

        // Lower should exclude the boundary (only order2 has item < boundaryDate)
        var queryLt = new ProjectionQuery();
        queryLt.Filters.Add(new Filter
        {
            PropertyName = "Items.AddedAt",
            Operator = FilterOperator.Lower,
            Value = boundaryDate
        });

        var ordersLt = await ProjectionsRepository.Query(queryLt);
        ordersLt.TotalRecordsFound.Should().Be(1, "Lower should exclude the boundary value");
    }

    [TestMethod]
    public virtual async Task TestProjectionsQueryFilterNumericGreaterVsGreaterOrEqual()
    {
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Order with exactly 2 items
        var order1Items = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Item A", 10.00m),
            new OrderItem(DateTime.UtcNow, "Item B", 20.00m)
        };
        var order1 = new Order(Guid.NewGuid(), "Two Items Order", order1Items, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order1);

        // Order with 3 items
        var order2Items = new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Item C", 10.00m),
            new OrderItem(DateTime.UtcNow, "Item D", 20.00m),
            new OrderItem(DateTime.UtcNow, "Item E", 30.00m)
        };
        var order2 = new Order(Guid.NewGuid(), "Three Items Order", order2Items, userId, "jane@gmail.com");
        await orderRepository.SaveOrder(userInfo, order2);

        await Task.Delay(ProjectionsUpdateDelay);

        // GreaterOrEqual 2 should include both orders
        var queryGte = new ProjectionQuery();
        queryGte.Filters.Add(new Filter
        {
            PropertyName = "ItemsCount",
            Operator = FilterOperator.GreaterOrEqual,
            Value = 2L
        });

        var ordersGte = await ProjectionsRepository.Query(queryGte);
        ordersGte.TotalRecordsFound.Should().Be(2, "GreaterOrEqual should include the boundary value");

        // Greater 2 should only include the 3-item order
        var queryGt = new ProjectionQuery();
        queryGt.Filters.Add(new Filter
        {
            PropertyName = "ItemsCount",
            Operator = FilterOperator.Greater,
            Value = 2L
        });

        var ordersGt = await ProjectionsRepository.Query(queryGt);
        ordersGt.TotalRecordsFound.Should().Be(1, "Greater should exclude the boundary value");
    }

    [TestMethod]
    public virtual async Task TestProjectionsQueryFilterSameFieldTwice()
    {
        var orderRepository = new OrderRepository(await GetEventStore());

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        var order1 = new Order(Guid.NewGuid(), "Small Order", new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Item", 10.00m)
        }, userId, "john@gmail.com");
        await orderRepository.SaveOrder(userInfo, order1);

        var order2 = new Order(Guid.NewGuid(), "Medium Order", new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "Item A", 10.00m),
            new OrderItem(DateTime.UtcNow, "Item B", 20.00m),
            new OrderItem(DateTime.UtcNow, "Item C", 30.00m)
        }, userId, "jane@gmail.com");
        await orderRepository.SaveOrder(userInfo, order2);

        var order3 = new Order(Guid.NewGuid(), "Large Order", new List<OrderItem>
        {
            new OrderItem(DateTime.UtcNow, "I1", 1m),
            new OrderItem(DateTime.UtcNow, "I2", 2m),
            new OrderItem(DateTime.UtcNow, "I3", 3m),
            new OrderItem(DateTime.UtcNow, "I4", 4m),
            new OrderItem(DateTime.UtcNow, "I5", 5m)
        }, userId, "bob@gmail.com");
        await orderRepository.SaveOrder(userInfo, order3);

        await Task.Delay(ProjectionsUpdateDelay);

        // Filter: ItemsCount >= 2 AND ItemsCount <= 4 (should match only order2 with 3 items)
        var query = new ProjectionQuery();
        query.Filters.Add(new Filter
        {
            PropertyName = "ItemsCount",
            Operator = FilterOperator.GreaterOrEqual,
            Value = 2L
        });
        query.Filters.Add(new Filter
        {
            PropertyName = "ItemsCount",
            Operator = FilterOperator.LowerOrEqual,
            Value = 4L
        });

        var orders = await ProjectionsRepository.Query(query);
        orders.TotalRecordsFound.Should().Be(1);
        orders.Records.First().Document!.Name.Should().Be("Medium Order");
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

    #region Batch Mode Tests

    [TestMethod]
    public async Task TestBatchUpsert_ReadYourWrites()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create order and wait for projection
        var order = new Order(Guid.NewGuid(), "Batch RYW Order", new List<OrderItem>(), userId, "john@gmail.com");
        await orderRepository.SaveAsync(userInfo, order);
        await Task.Delay(ProjectionsUpdateDelay);

        // Enable batch mode
        factory.BeginBatchOnAll();
        try
        {
            // Add item triggers Single (read) then Upsert (write) in builder
            order.AddItem(new OrderItem(DateTime.UtcNow, "TestItem", 10.00m));
            await orderRepository.SaveAsync(userInfo, order);
            await Task.Delay(ProjectionsUpdateDelay);

            // In batch mode, the projection should be in the buffer
            // Single() should see the buffered version (read-your-writes)
            var projInBuffer = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
            projInBuffer.Should().NotBeNull();
            projInBuffer!.ItemsCount.Should().Be(1);

            // Flush and verify it's persisted
            await factory.FlushBatchOnAllAsync();
        }
        finally
        {
            factory.EndBatchOnAll();
        }

        var proj = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull();
        proj!.ItemsCount.Should().Be(1);
    }

    [TestMethod]
    public async Task TestBatchUpsert_DeleteInBatch()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create two orders
        var order1 = new Order(Guid.NewGuid(), "Delete Batch Order 1", new List<OrderItem>(), userId, "john@gmail.com");
        var order2 = new Order(Guid.NewGuid(), "Delete Batch Order 2", new List<OrderItem>(), userId, "john@gmail.com");
        await orderRepository.SaveAsync(userInfo, order1);
        await orderRepository.SaveAsync(userInfo, order2);
        await Task.Delay(ProjectionsUpdateDelay);

        // Enable batch, delete one order
        factory.BeginBatchOnAll();
        try
        {
            await repo.Delete(order1.Id, PartitionKeys.GetOrderPartitionKey());

            // In batch: deleted order should not be visible via Single
            var deletedProj = await repo.Single(order1.Id, PartitionKeys.GetOrderPartitionKey());
            deletedProj.Should().BeNull();

            // Non-deleted order still visible
            var existingProj = await repo.Single(order2.Id, PartitionKeys.GetOrderPartitionKey());
            existingProj.Should().NotBeNull();

            await factory.FlushBatchOnAllAsync();
        }
        finally
        {
            factory.EndBatchOnAll();
        }

        // After flush, verify deletion persisted
        var proj1 = await repo.Single(order1.Id, PartitionKeys.GetOrderPartitionKey());
        proj1.Should().BeNull();

        var proj2 = await repo.Single(order2.Id, PartitionKeys.GetOrderPartitionKey());
        proj2.Should().NotBeNull();
    }

    [TestMethod]
    public async Task TestBatchUpsert_MultipleOrdersFlushed()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        factory.BeginBatchOnAll();
        try
        {
            // Create 10 orders in batch mode
            for (int i = 0; i < 10; i++)
            {
                var order = new Order(Guid.NewGuid(), $"Batch Order {i}", new List<OrderItem>(), userId, "john@gmail.com");
                await orderRepository.SaveAsync(userInfo, order);
            }

            await Task.Delay(ProjectionsUpdateDelay);
            await factory.FlushBatchOnAllAsync();
        }
        finally
        {
            factory.EndBatchOnAll();
        }

        // Allow ES to refresh after flush
        await Task.Delay(ProjectionsUpdateDelay);

        // All 10 orders should be queryable
        var results = await repo.Query(new ProjectionQuery() { Limit = 20 });
        results.TotalRecordsFound.Should().Be(10);
    }

    [TestMethod]
    public async Task TestSaveMultipleNewAsync()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new AggregateRepository<Order>(eventStore);
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Enable batch mode so that handler-triggered upserts are buffered
        factory.BeginBatchOnAll();
        try
        {
            var orders = new List<Order>();
            for (int i = 0; i < 5; i++)
            {
                orders.Add(new Order(Guid.NewGuid(), $"Bulk Order {i}", new List<OrderItem>(), userId, "john@gmail.com"));
            }

            await orderRepository.SaveMultipleNewAsync(userInfo, orders);
            await Task.Delay(ProjectionsUpdateDelay);
            await factory.FlushBatchOnAllAsync();
        }
        finally
        {
            factory.EndBatchOnAll();
        }

        // Allow ES to refresh after flush
        await Task.Delay(ProjectionsUpdateDelay);

        // All 5 orders should be persisted in event store and projections
        var results = await repo.Query(new ProjectionQuery() { Limit = 20 });
        results.TotalRecordsFound.Should().Be(5);

        // Verify events are in the event store
        for (int i = 0; i < 5; i++)
        {
            var loaded = await repo.Query(new ProjectionQuery
            {
                Filters = new List<Filter>
                {
                    new Filter(nameof(OrderListProjectionItem.Name), FilterOperator.Equal, $"Bulk Order {i}")
                }
            });
            loaded.TotalRecordsFound.Should().Be(1);
        }
    }

    [TestMethod]
    public async Task TestBatchRebuild()
    {
        var orderRepository = new AggregateRepository<Order>(await GetEventStore());
        var factory = GetProjectionRepositoryFactory();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // Create orders normally
        for (int i = 0; i < 5; i++)
        {
            var order = new Order(Guid.NewGuid(), $"Rebuild Order {i}", new List<OrderItem>(), userId, "john@gmail.com");
            await orderRepository.SaveAsync(userInfo, order);
        }

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify all projections exist
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var results = await repo.Query(new ProjectionQuery() { Limit = 20 });
        results.TotalRecordsFound.Should().Be(5);

        // Delete projections and rebuild with batch mode
        await repo.DeleteAll();
        await repo.EnsureIndex();

        // Create a new RebuildProcessor with factory for batch mode
        var rebuildProcessor = new ProjectionsRebuildProcessor(
            factory.GetProjectionsIndexStateRepository(),
            async (string connectionId) =>
            {
                var rebuildEngine = new ProjectionsEngine(GetEventStoreEventsObserver());
                var builder = new OrdersListProjectionBuilder(factory, ProjectionOperationIndexSelector.ProjectionRebuild);
                rebuildEngine.AddProjectionBuilder(builder);
                return rebuildEngine;
            },
            NullLogger<ProjectionsRebuildProcessor>.Instance,
            factory
        );

        await rebuildProcessor.RebuildProjectionsThatRequireRebuild();

        // Allow ES to refresh after rebuild flush
        await Task.Delay(ProjectionsUpdateDelay);

        // All 5 projections should be rebuilt
        results = await repo.Query(new ProjectionQuery() { Limit = 20 });
        results.TotalRecordsFound.Should().Be(5);
    }

    #endregion

    #region Batch Performance Tests

    [TestMethod]
    public async Task TestBatchImport_Performance()
    {
        const int totalAggregates = 10_000;
        const int chunkSize = 1000;
        const int itemsPerOrder = 3;

        var eventStore = await GetEventStore();
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var orderRepository = new AggregateRepository<Order>(eventStore);

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // --- Phase 1: Chunked batch import (realistic scenario) ---
        factory.BeginBatchOnAll();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var eventStoreTotal = TimeSpan.Zero;
        var projectionFlushTotal = TimeSpan.Zero;
        var objectCreationTotal = TimeSpan.Zero;

        for (int chunk = 0; chunk < totalAggregates; chunk += chunkSize)
        {
            var chunkSw = System.Diagnostics.Stopwatch.StartNew();
            var currentChunkSize = Math.Min(chunkSize, totalAggregates - chunk);
            var orders = new List<Order>(currentChunkSize);
            for (int i = 0; i < currentChunkSize; i++)
            {
                var idx = chunk + i;
                var items = Enumerable.Range(0, itemsPerOrder)
                    .Select(j => new OrderItem(DateTime.UtcNow, $"Item {j} of order {idx}", 10.0m + j))
                    .ToList();
                orders.Add(new Order(Guid.NewGuid(), $"Perf Order {idx}", items, userId, $"user{idx}@test.com"));
            }
            objectCreationTotal += chunkSw.Elapsed;

            chunkSw.Restart();
            // SaveMultipleNewAsync = event store insert + handler notification (buffered upserts)
            await orderRepository.SaveMultipleNewAsync(userInfo, orders);
            eventStoreTotal += chunkSw.Elapsed;

            chunkSw.Restart();
            await factory.FlushBatchOnAllAsync();
            projectionFlushTotal += chunkSw.Elapsed;
        }

        var batchTotalElapsed = sw.Elapsed;
        factory.EndBatchOnAll();

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify all projections written
        var results = await repo.Query(new ProjectionQuery() { Limit = 1 });
        results.TotalRecordsFound.Should().Be(totalAggregates);

        // --- Phase 2: Rebuild from scratch ---
        await repo.DeleteAll();
        await repo.EnsureIndex();

        var rebuildProcessor = new ProjectionsRebuildProcessor(
            factory.GetProjectionsIndexStateRepository(),
            async (string connectionId) =>
            {
                var rebuildEngine = new ProjectionsEngine(GetEventStoreEventsObserver());
                var builder = new OrdersListProjectionBuilder(factory, ProjectionOperationIndexSelector.ProjectionRebuild);
                rebuildEngine.AddProjectionBuilder(builder);
                return rebuildEngine;
            },
            NullLogger<ProjectionsRebuildProcessor>.Instance,
            factory
        );

        sw.Restart();
        await rebuildProcessor.RebuildProjectionsThatRequireRebuild();
        var rebuildElapsed = sw.Elapsed;

        await Task.Delay(ProjectionsUpdateDelay);

        results = await repo.Query(new ProjectionQuery() { Limit = 1 });
        results.TotalRecordsFound.Should().Be(totalAggregates);

        // --- Phase 3: Sequential insert for comparison (smaller sample) ---
        const int sequentialCount = 200;
        await eventStore.DeleteAll();
        await repo.DeleteAll();
        await repo.EnsureIndex();

        sw.Restart();
        for (int i = 0; i < sequentialCount; i++)
        {
            var items = Enumerable.Range(0, itemsPerOrder)
                .Select(j => new OrderItem(DateTime.UtcNow, $"Seq Item {j}", 5.0m + j))
                .ToList();
            var order = new Order(Guid.NewGuid(), $"Seq Order {i}", items, userId, $"seq{i}@test.com");
            await orderRepository.SaveAsync(userInfo, order);
        }
        var sequentialElapsed = sw.Elapsed;

        await Task.Delay(ProjectionsUpdateDelay);

        results = await repo.Query(new ProjectionQuery() { Limit = 1 });
        results.TotalRecordsFound.Should().Be(sequentialCount);

        var seqPerAgg = sequentialElapsed.TotalMilliseconds / sequentialCount;
        var estimatedSequential = seqPerAgg * totalAggregates;

        // Output timing results
        Console.WriteLine($"=== Batch Performance Test ({totalAggregates} aggregates, {itemsPerOrder} items each, chunks of {chunkSize}) ===");
        Console.WriteLine($"Object creation:             {objectCreationTotal.TotalMilliseconds:F0}ms");
        Console.WriteLine($"Event store + handlers:      {eventStoreTotal.TotalMilliseconds:F0}ms");
        Console.WriteLine($"Projection flush:            {projectionFlushTotal.TotalMilliseconds:F0}ms");
        Console.WriteLine($"Batch import total:          {batchTotalElapsed.TotalMilliseconds:F0}ms ({totalAggregates / batchTotalElapsed.TotalSeconds:F0} agg/s)");
        Console.WriteLine($"Rebuild:                     {rebuildElapsed.TotalMilliseconds:F0}ms ({totalAggregates / rebuildElapsed.TotalSeconds:F0} agg/s)");
        Console.WriteLine($"Sequential insert ({sequentialCount} agg):    {sequentialElapsed.TotalMilliseconds:F0}ms ({sequentialCount / sequentialElapsed.TotalSeconds:F0} agg/s)");
        Console.WriteLine($"Estimated sequential {totalAggregates}:     {estimatedSequential:F0}ms");
        Console.WriteLine($"Batch speedup:               {estimatedSequential / batchTotalElapsed.TotalMilliseconds:F1}x");
        Console.WriteLine($"--- Extrapolation to 1M aggregates ---");
        Console.WriteLine($"Batch (estimated):           {batchTotalElapsed.TotalMilliseconds / totalAggregates * 1_000_000 / 1000:F0}s");
        Console.WriteLine($"Sequential (estimated):      {seqPerAgg * 1_000_000 / 1000:F0}s ({seqPerAgg * 1_000_000 / 3600000:F1}h)");
    }

    [TestMethod]
    public virtual async Task TestBatchUpdateExisting()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new AggregateRepository<Order>(eventStore);
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        // 1. Create 100 orders via batch insert
        factory.BeginBatchOnAll();
        var orders = new List<Order>();
        for (int i = 0; i < 100; i++)
        {
            var items = new List<OrderItem>
            {
                new OrderItem(DateTime.UtcNow, $"Item of order {i}", 10.0m + i)
            };
            orders.Add(new Order(Guid.NewGuid(), $"Order {i}", items, userId, $"user{i}@test.com"));
        }
        await orderRepository.SaveMultipleNewAsync(userInfo, orders);
        await factory.FlushBatchOnAllAsync();
        factory.EndBatchOnAll();

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify all 100 created
        var results = await repo.Query(new ProjectionQuery() { Limit = 1 });
        results.TotalRecordsFound.Should().Be(100);

        // 2. "Excel update": compare with projections, update every other order
        factory.BeginBatchOnAll();
        try
        {
            var updates = new List<(Guid StreamId, string PartitionKey, IReadOnlyList<IEvent> Events)>();

            for (int i = 0; i < orders.Count; i++)
            {
                var order = orders[i];
                var newName = $"Updated Order {i}";

                // Compare with current projection (simulating Excel import)
                var currentProjection = await repo.Single(order.Id, order.PartitionKey);
                currentProjection.Should().NotBeNull();

                var currentName = currentProjection!.Name;
                if (i % 2 == 0 && currentName != newName)
                {
                    updates.Add((
                        order.Id,
                        order.PartitionKey,
                        new List<IEvent> { new OrderNameUpdated(order.Id, newName, order.PartitionKey) }
                    ));
                }
            }

            updates.Count.Should().Be(50);

            await orderRepository.AppendEventsToMultipleAsync(userInfo, updates);
            await factory.FlushBatchOnAllAsync();
        }
        finally
        {
            factory.EndBatchOnAll();
        }

        await Task.Delay(ProjectionsUpdateDelay);

        // 3. Verify: 100 total, 50 updated, 50 unchanged
        results = await repo.Query(new ProjectionQuery() { Limit = 200 });
        results.TotalRecordsFound.Should().Be(100);

        var updatedCount = results.Records
            .Count(r => r.Document!.Name.StartsWith("Updated Order"));
        updatedCount.Should().Be(50);

        var unchangedCount = results.Records
            .Count(r => r.Document!.Name.StartsWith("Order "));
        unchangedCount.Should().Be(50);

        // 4. Verify aggregate state: load one updated order
        var updatedOrder = orders[0]; // i=0, should be updated
        var loadedStream = await eventStore.LoadStreamAsync(updatedOrder.Id, updatedOrder.PartitionKey);
        var loadedOrder = new Order(loadedStream.Events);
        loadedOrder.OrderName.Should().Be("Updated Order 0");

        // 5. Verify aggregate state: load one unchanged order
        var unchangedOrder = orders[1]; // i=1, should be unchanged
        loadedStream = await eventStore.LoadStreamAsync(unchangedOrder.Id, unchangedOrder.PartitionKey);
        loadedOrder = new Order(loadedStream.Events);
        loadedOrder.OrderName.Should().Be("Order 1");
    }

    [TestMethod]
    public virtual async Task TestBatchUpdate_Performance()
    {
        var eventStore = await GetEventStore();
        var orderRepository = new AggregateRepository<Order>(eventStore);
        var factory = GetProjectionRepositoryFactory();
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);

        const int totalAggregates = 10_000;
        const int updateCount = 5_000;
        const int chunkSize = 1000;

        // 1. Create orders via batch insert
        factory.BeginBatchOnAll();
        var orders = new List<Order>(totalAggregates);
        for (int i = 0; i < totalAggregates; i++)
        {
            var items = new List<OrderItem>
            {
                new OrderItem(DateTime.UtcNow, $"Item {i}", 10.0m)
            };
            orders.Add(new Order(Guid.NewGuid(), $"Order {i}", items, userId, $"user{i}@test.com"));
        }

        foreach (var chunk in orders.Chunk(chunkSize))
        {
            await orderRepository.SaveMultipleNewAsync(userInfo, chunk.ToList());
            await factory.FlushBatchOnAllAsync();
        }
        factory.EndBatchOnAll();

        await Task.Delay(ProjectionsUpdateDelay);

        // 2. Batch update first N orders
        var ordersToUpdate = orders.Take(updateCount).ToList();

        factory.BeginBatchOnAll();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var eventStoreTotal = TimeSpan.Zero;
        var projectionFlushTotal = TimeSpan.Zero;

        try
        {
            foreach (var chunk in ordersToUpdate.Chunk(chunkSize))
            {
                var updates = chunk.Select(o => (
                    StreamId: o.Id,
                    PartitionKey: o.PartitionKey,
                    Events: (IReadOnlyList<IEvent>)new List<IEvent>
                    {
                        new OrderNameUpdated(o.Id, $"Updated {o.OrderName}", o.PartitionKey)
                    }
                )).ToList();

                var chunkSw = System.Diagnostics.Stopwatch.StartNew();
                await orderRepository.AppendEventsToMultipleAsync(userInfo, updates);
                eventStoreTotal += chunkSw.Elapsed;

                chunkSw.Restart();
                await factory.FlushBatchOnAllAsync();
                projectionFlushTotal += chunkSw.Elapsed;
            }
        }
        finally
        {
            factory.EndBatchOnAll();
        }
        var batchUpdateElapsed = sw.Elapsed;

        await Task.Delay(ProjectionsUpdateDelay);

        // Verify
        var results = await repo.Query(new ProjectionQuery() { Limit = 1 });
        results.TotalRecordsFound.Should().Be(totalAggregates);

        // 3. Sequential update for comparison (small sample)
        const int sequentialCount = 100;
        var aggregateType = typeof(Order).AssemblyQualifiedName ?? "";
        sw.Restart();
        for (int i = updateCount; i < updateCount + sequentialCount && i < totalAggregates; i++)
        {
            var o = orders[i];
            var loaded = new Order(
                (await eventStore.LoadStreamAsync(o.Id, o.PartitionKey)).Events
            );
            var evt = new OrderNameUpdated(o.Id, $"Seq Updated {o.OrderName}", o.PartitionKey);
            evt.AggregateType = aggregateType;
            await eventStore.AppendToStreamAsync(
                userInfo, o.Id, loaded.Version,
                new List<IEvent> { evt }
            );
        }
        var sequentialElapsed = sw.Elapsed;

        var seqPerAgg = sequentialElapsed.TotalMilliseconds / sequentialCount;
        var estimatedSequential = seqPerAgg * updateCount;

        // Output timing results
        Console.WriteLine($"=== Batch Update Performance Test ({updateCount}/{totalAggregates} aggregates) ===");
        Console.WriteLine($"Event store + handlers:      {eventStoreTotal.TotalMilliseconds:F0}ms");
        Console.WriteLine($"Projection flush:            {projectionFlushTotal.TotalMilliseconds:F0}ms");
        Console.WriteLine($"Batch update total:          {batchUpdateElapsed.TotalMilliseconds:F0}ms ({updateCount / batchUpdateElapsed.TotalSeconds:F0} agg/s)");
        Console.WriteLine($"Sequential update ({sequentialCount} agg):   {sequentialElapsed.TotalMilliseconds:F0}ms ({sequentialCount / sequentialElapsed.TotalSeconds:F0} agg/s)");
        Console.WriteLine($"Estimated sequential {updateCount}:    {estimatedSequential:F0}ms");
        Console.WriteLine($"Batch speedup:               {estimatedSequential / batchUpdateElapsed.TotalMilliseconds:F1}x");
        Console.WriteLine($"--- Extrapolation to 1M updates ---");
        Console.WriteLine($"Batch (estimated):           {batchUpdateElapsed.TotalMilliseconds / updateCount * 1_000_000 / 1000:F0}s");
        Console.WriteLine($"Sequential (estimated):      {seqPerAgg * 1_000_000 / 1000:F0}s ({seqPerAgg * 1_000_000 / 3600000:F1}h)");
    }

    #endregion
}