using System.Collections.Concurrent;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;
using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.EventSourcing.Tests.Domain.Projections.OrdersListProjection;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;
using CloudFabric.Projections;
using CloudFabric.Projections.InMemory;
using CloudFabric.Projections.Worker;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.InMemory;

[TestClass]
public class ProjectionsInfrastructureTests
{
    private InMemoryEventStore CreateEventStore()
    {
        var store = new InMemoryEventStore(
            new ConcurrentDictionary<(Guid, string), List<string>>()
        );
        store.Initialize().GetAwaiter().GetResult();
        return store;
    }

    private InMemoryEventStoreEventObserver CreateObserver(InMemoryEventStore store)
    {
        return new InMemoryEventStoreEventObserver(store, NullLogger<InMemoryEventStoreEventObserver>.Instance);
    }

    private ProjectionRepositoryFactory CreateProjectionRepositoryFactory()
    {
        return new InMemoryProjectionRepositoryFactory(NullLoggerFactory.Instance);
    }

    private OrdersListProjectionBuilder CreateProjectionBuilder(
        ProjectionRepositoryFactory factory,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    )
    {
        return new OrdersListProjectionBuilder(factory, indexSelector);
    }

    private async Task EnsureProjectionIndexReady(
        ProjectionRepositoryFactory factory,
        EventsObserver observer
    )
    {
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        await repo.EnsureIndex();

        var rebuildProcessor = new ProjectionsRebuildProcessor(
            factory.GetProjectionsIndexStateRepository(),
            async (_) =>
            {
                var engine = new ProjectionsEngine(observer);
                engine.AddProjectionBuilder(
                    CreateProjectionBuilder(factory, ProjectionOperationIndexSelector.ProjectionRebuild)
                );
                return engine;
            },
            NullLogger<ProjectionsRebuildProcessor>.Instance
        );

        await rebuildProcessor.RebuildProjectionsThatRequireRebuild();
    }

    private async Task<Order> CreateAndSaveOrder(IEventStore store, string name = "Test Order")
    {
        var repo = new AggregateRepository<Order>(store);
        var userId = Guid.NewGuid();
        var userInfo = new EventUserInfo(userId);
        var items = new List<OrderItem>
        {
            new(DateTime.UtcNow, "Item1", 10.00m)
        };
        var order = new Order(Guid.NewGuid(), name, items, userId, "test@test.com");
        await repo.SaveAsync(userInfo, order);
        return order;
    }

    #region Multi-handler EventsObserver

    [TestMethod]
    public async Task TwoEnginesOnSameObserver_BothReceiveLiveEvents()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory1 = CreateProjectionRepositoryFactory();
        var factory2 = CreateProjectionRepositoryFactory();

        var engine1 = new ProjectionsEngine(observer);
        engine1.AddProjectionBuilder(CreateProjectionBuilder(factory1));

        var engine2 = new ProjectionsEngine(observer);
        engine2.AddProjectionBuilder(CreateProjectionBuilder(factory2));

        await engine1.StartAsync("engine1");
        await engine2.StartAsync("engine2");

        await EnsureProjectionIndexReady(factory1, observer);
        await EnsureProjectionIndexReady(factory2, observer);

        var order = await CreateAndSaveOrder(store);
        await Task.Delay(500);

        var repo1 = factory1.GetProjectionRepository<OrderListProjectionItem>();
        var repo2 = factory2.GetProjectionRepository<OrderListProjectionItem>();

        var proj1 = await repo1.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        var proj2 = await repo2.Single(order.Id, PartitionKeys.GetOrderPartitionKey());

        proj1.Should().NotBeNull("engine1 should receive the live event");
        proj2.Should().NotBeNull("engine2 should receive the live event");
        proj1!.Name.Should().Be("Test Order");
        proj2!.Name.Should().Be("Test Order");

        await engine1.StopAsync();
        await engine2.StopAsync();
    }

    [TestMethod]
    public async Task RemoveEventHandler_StopsDeliveryToRemovedEngine()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory1 = CreateProjectionRepositoryFactory();
        var factory2 = CreateProjectionRepositoryFactory();

        var engine1 = new ProjectionsEngine(observer);
        engine1.AddProjectionBuilder(CreateProjectionBuilder(factory1));

        var engine2 = new ProjectionsEngine(observer);
        engine2.AddProjectionBuilder(CreateProjectionBuilder(factory2));

        await engine1.StartAsync("engine1");
        await engine2.StartAsync("engine2");

        await EnsureProjectionIndexReady(factory1, observer);
        await EnsureProjectionIndexReady(factory2, observer);

        // Stop engine1 — its handler should be removed
        await engine1.StopAsync();

        // Re-subscribe the observer since StopAsync unsubscribed it
        await observer.StartAsync("restarted");

        var order = await CreateAndSaveOrder(store, "After Stop");
        await Task.Delay(500);

        var repo1 = factory1.GetProjectionRepository<OrderListProjectionItem>();
        var repo2 = factory2.GetProjectionRepository<OrderListProjectionItem>();

        var proj1 = await repo1.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        var proj2 = await repo2.Single(order.Id, PartitionKeys.GetOrderPartitionKey());

        proj1.Should().BeNull("engine1 was stopped and should not receive events");
        proj2.Should().NotBeNull("engine2 is still running and should receive events");

        await engine2.StopAsync();
    }

    [TestMethod]
    public async Task ReplayEvents_GoOnlyToRequestingEngine()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory1 = CreateProjectionRepositoryFactory();
        var factory2 = CreateProjectionRepositoryFactory();

        // Engine1 — live engine
        var engine1 = new ProjectionsEngine(observer);
        engine1.AddProjectionBuilder(CreateProjectionBuilder(factory1));
        await engine1.StartAsync("live");
        await EnsureProjectionIndexReady(factory1, observer);

        // Save an order — engine1 picks it up via live events
        var order = await CreateAndSaveOrder(store, "Live Order");
        await Task.Delay(500);

        var repo1 = factory1.GetProjectionRepository<OrderListProjectionItem>();
        var proj1Before = await repo1.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj1Before.Should().NotBeNull();
        proj1Before!.ItemsCount.Should().Be(1);

        // Engine2 — replays all events from scratch
        await EnsureProjectionIndexReady(factory2, observer);
        var engine2 = new ProjectionsEngine(observer);
        engine2.AddProjectionBuilder(CreateProjectionBuilder(factory2));

        await engine2.ReplayEventsAsync("rebuild", null, null);

        var repo2 = factory2.GetProjectionRepository<OrderListProjectionItem>();
        var proj2 = await repo2.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj2.Should().NotBeNull("replay should populate engine2's projection");

        // Re-read engine1's projection — should still have the same count (not doubled)
        var proj1After = await repo1.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj1After!.ItemsCount.Should().Be(1,
            "engine1 should have processed the event only once via live dispatch, not again via replay");

        await engine1.StopAsync();
        await engine2.DisposeAsync();
    }

    [TestMethod]
    public async Task RebuildOneAsync_GoesOnlyToRequestingEngine()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var engine = new ProjectionsEngine(observer);
        engine.AddProjectionBuilder(CreateProjectionBuilder(factory));
        await engine.StartAsync("live");
        await EnsureProjectionIndexReady(factory, observer);

        var order = await CreateAndSaveOrder(store, "Rebuild One");
        await Task.Delay(500);

        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var projBefore = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        projBefore.Should().NotBeNull();

        // Delete the projection
        await repo.Delete(order.Id, PartitionKeys.GetOrderPartitionKey());
        var deleted = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        deleted.Should().BeNull();

        // Rebuild one document — should restore projection
        await engine.RebuildOneAsync(order.Id, PartitionKeys.GetOrderPartitionKey());

        var rebuilt = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        rebuilt.Should().NotBeNull();
        rebuilt!.Name.Should().Be("Rebuild One");

        await engine.StopAsync();
    }

    #endregion

    #region IProjectionErrorHandler

    [TestMethod]
    public async Task ErrorHandler_CalledWhenProjectionBuilderThrows()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var errors = new ConcurrentBag<(IProjectionBuilder builder, IEvent @event, Exception ex)>();
        var errorHandler = new LogAndContinueProjectionErrorHandler((b, e, ex) =>
        {
            errors.Add((b, e, ex));
        });

        var engine = new ProjectionsEngine(observer, NullLogger<ProjectionsEngine>.Instance, errorHandler);
        engine.AddProjectionBuilder(new ThrowingProjectionBuilder(factory));
        await engine.StartAsync("error-test");

        var order = await CreateAndSaveOrder(store);
        await Task.Delay(500);

        errors.Should().NotBeEmpty("error handler should have been called");
        errors.First().ex.Message.Should().Contain("Intentional test error");

        await engine.StopAsync();
    }

    [TestMethod]
    public async Task ErrorHandler_EngineContinuesProcessingOtherBuilders()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var errors = new ConcurrentBag<(IProjectionBuilder builder, IEvent @event, Exception ex)>();
        var errorHandler = new LogAndContinueProjectionErrorHandler((b, e, ex) =>
        {
            errors.Add((b, e, ex));
        });

        var engine = new ProjectionsEngine(observer, NullLogger<ProjectionsEngine>.Instance, errorHandler);

        // Add a throwing builder first
        engine.AddProjectionBuilder(new ThrowingProjectionBuilder(factory));
        // Add a working builder second
        engine.AddProjectionBuilder(CreateProjectionBuilder(factory));

        await engine.StartAsync("continue-test");
        await EnsureProjectionIndexReady(factory, observer);

        var order = await CreateAndSaveOrder(store);
        await Task.Delay(500);

        // The throwing builder should have errored
        errors.Should().NotBeEmpty();

        // But the working builder should still have processed the event
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var proj = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull("working builder should process event even if previous builder threw");

        await engine.StopAsync();
    }

    #endregion

    #region ProjectionsEngineBuilder

    [TestMethod]
    public void Builder_WithoutObserver_ThrowsOnBuild()
    {
        var act = () => ProjectionsEngine.CreateBuilder().Build();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*EventsObserver*");
    }

    [TestMethod]
    public async Task Builder_CreatesWorkingEngine()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var engine = ProjectionsEngine.CreateBuilder()
            .WithEventsObserver(observer)
            .AddProjectionBuilder(CreateProjectionBuilder(factory))
            .Build();

        await engine.StartAsync("builder-test");
        await EnsureProjectionIndexReady(factory, observer);

        var order = await CreateAndSaveOrder(store);
        await Task.Delay(500);

        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var proj = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull();
        proj!.Name.Should().Be("Test Order");

        await engine.StopAsync();
    }

    [TestMethod]
    public async Task Builder_WithErrorHandler_IntegratesCorrectly()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var errorsCaught = 0;
        var errorHandler = new LogAndContinueProjectionErrorHandler((_, _, _) =>
        {
            Interlocked.Increment(ref errorsCaught);
        });

        var engine = ProjectionsEngine.CreateBuilder()
            .WithEventsObserver(observer)
            .AddProjectionBuilder(new ThrowingProjectionBuilder(factory))
            .WithErrorHandler(errorHandler)
            .Build();

        await engine.StartAsync("builder-error-test");

        await CreateAndSaveOrder(store);
        await Task.Delay(500);

        errorsCaught.Should().BeGreaterThan(0, "error handler should be called through builder-created engine");

        await engine.StopAsync();
    }

    #endregion

    #region IAsyncDisposable

    [TestMethod]
    public async Task DisposeAsync_RemovesHandler_EngineStopsReceiving()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        Guid order1Id;

        await using (var engine = new ProjectionsEngine(observer))
        {
            engine.AddProjectionBuilder(CreateProjectionBuilder(factory));
            await engine.StartAsync("dispose-test");
            await EnsureProjectionIndexReady(factory, observer);

            var order1 = await CreateAndSaveOrder(store, "Before Dispose");
            order1Id = order1.Id;
            await Task.Delay(500);

            var proj = await repo.Single(order1Id, PartitionKeys.GetOrderPartitionKey());
            proj.Should().NotBeNull("engine should process events before dispose");
        }
        // engine is now disposed — handler removed, but observer still alive

        // Create a new engine to keep the observer active (otherwise no handlers = error on event)
        var factory2 = CreateProjectionRepositoryFactory();
        var keepAliveEngine = new ProjectionsEngine(observer);
        keepAliveEngine.AddProjectionBuilder(CreateProjectionBuilder(factory2));
        await observer.StartAsync("keepalive");

        var order2 = await CreateAndSaveOrder(store, "After Dispose");
        await Task.Delay(500);

        // The disposed engine's projection should NOT have the new order
        var projAfter = await repo.Single(order2.Id, PartitionKeys.GetOrderPartitionKey());
        projAfter.Should().BeNull("disposed engine should not receive new events");

        await keepAliveEngine.StopAsync();
    }

    [TestMethod]
    public async Task DisposeAsync_DoesNotStopObserver()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory1 = CreateProjectionRepositoryFactory();
        var factory2 = CreateProjectionRepositoryFactory();

        var engine2 = new ProjectionsEngine(observer);
        engine2.AddProjectionBuilder(CreateProjectionBuilder(factory2));

        await using (var engine1 = new ProjectionsEngine(observer))
        {
            engine1.AddProjectionBuilder(CreateProjectionBuilder(factory1));
            await engine1.StartAsync("engine1");
            await engine2.StartAsync("engine2");

            await EnsureProjectionIndexReady(factory1, observer);
            await EnsureProjectionIndexReady(factory2, observer);
        }
        // engine1 is disposed, but observer should still be alive for engine2

        var order = await CreateAndSaveOrder(store, "After Engine1 Disposed");
        await Task.Delay(500);

        var repo2 = factory2.GetProjectionRepository<OrderListProjectionItem>();
        var proj2 = await repo2.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj2.Should().NotBeNull("engine2 should still receive events after engine1 is disposed");

        await engine2.StopAsync();
    }

    #endregion

    #region CleanupStaleIndicesAsync

    [TestMethod]
    public async Task CleanupStaleIndices_RemovesOldCompletedIndices()
    {
        var factory = CreateProjectionRepositoryFactory();
        var repo = factory.GetProjectionsIndexStateRepository();
        await repo.EnsureIndex();

        var state = new ProjectionIndexState
        {
            Id = Guid.NewGuid(),
            ProjectionName = "test_projection",
            ConnectionId = "",
            IndexesStatuses = new List<IndexStateForSchemaVersion>
            {
                new()
                {
                    IndexName = "test_projection_old",
                    SchemaHash = "hash_old",
                    Schema = "{}",
                    CreatedAt = DateTime.UtcNow.AddHours(-10),
                    RebuildStartedAt = DateTime.UtcNow.AddHours(-10),
                    RebuildCompletedAt = DateTime.UtcNow.AddHours(-9),
                    RebuildHealthCheckAt = DateTime.UtcNow.AddHours(-9)
                },
                new()
                {
                    IndexName = "test_projection_new",
                    SchemaHash = "hash_new",
                    Schema = "{}",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-30),
                    RebuildStartedAt = DateTime.UtcNow.AddMinutes(-30),
                    RebuildCompletedAt = DateTime.UtcNow.AddMinutes(-20),
                    RebuildHealthCheckAt = DateTime.UtcNow.AddMinutes(-20)
                }
            }
        };

        await repo.SaveProjectionIndexState(state);

        var droppedCount = await repo.CleanupStaleIndicesAsync(TimeSpan.FromHours(1));
        droppedCount.Should().Be(1);
    }

    [TestMethod]
    public async Task CleanupStaleIndices_KeepsFreshIndices()
    {
        var factory = CreateProjectionRepositoryFactory();
        var repo = factory.GetProjectionsIndexStateRepository();
        await repo.EnsureIndex();

        var state = new ProjectionIndexState
        {
            Id = Guid.NewGuid(),
            ProjectionName = "test_fresh",
            ConnectionId = "",
            IndexesStatuses = new List<IndexStateForSchemaVersion>
            {
                new()
                {
                    IndexName = "test_fresh_v1",
                    SchemaHash = "hash1",
                    Schema = "{}",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-30),
                    RebuildStartedAt = DateTime.UtcNow.AddMinutes(-30),
                    RebuildCompletedAt = DateTime.UtcNow.AddMinutes(-20),
                    RebuildHealthCheckAt = DateTime.UtcNow.AddMinutes(-20)
                },
                new()
                {
                    IndexName = "test_fresh_v2",
                    SchemaHash = "hash2",
                    Schema = "{}",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                    RebuildStartedAt = DateTime.UtcNow.AddMinutes(-10),
                    RebuildCompletedAt = DateTime.UtcNow.AddMinutes(-5),
                    RebuildHealthCheckAt = DateTime.UtcNow.AddMinutes(-5)
                }
            }
        };

        await repo.SaveProjectionIndexState(state);

        var droppedCount = await repo.CleanupStaleIndicesAsync(TimeSpan.FromHours(1));
        droppedCount.Should().Be(0, "both indices are within the grace period");
    }

    [TestMethod]
    public async Task CleanupStaleIndices_DoesNotRemoveSingleIndex()
    {
        var factory = CreateProjectionRepositoryFactory();
        var repo = factory.GetProjectionsIndexStateRepository();
        await repo.EnsureIndex();

        var state = new ProjectionIndexState
        {
            Id = Guid.NewGuid(),
            ProjectionName = "test_single",
            ConnectionId = "",
            IndexesStatuses = new List<IndexStateForSchemaVersion>
            {
                new()
                {
                    IndexName = "test_single_v1",
                    SchemaHash = "hash1",
                    Schema = "{}",
                    CreatedAt = DateTime.UtcNow.AddDays(-30),
                    RebuildStartedAt = DateTime.UtcNow.AddDays(-30),
                    RebuildCompletedAt = DateTime.UtcNow.AddDays(-30),
                    RebuildHealthCheckAt = DateTime.UtcNow.AddDays(-30)
                }
            }
        };

        await repo.SaveProjectionIndexState(state);

        var droppedCount = await repo.CleanupStaleIndicesAsync(TimeSpan.Zero);
        droppedCount.Should().Be(0, "single completed index should never be removed");
    }

    #endregion
}

/// <summary>
/// A projection builder that throws on every event, used to test error handling.
/// </summary>
public class ThrowingProjectionBuilder : ProjectionBuilder<OrderListProjectionItem>,
    IHandleEvent<OrderPlaced>,
    IHandleEvent<OrderItemAdded>
{
    public ThrowingProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    ) : base(projectionRepositoryFactory, indexSelector)
    {
    }

    public Task On(OrderPlaced evt)
    {
        throw new InvalidOperationException("Intentional test error in ThrowingProjectionBuilder");
    }

    public Task On(OrderItemAdded evt)
    {
        throw new InvalidOperationException("Intentional test error in ThrowingProjectionBuilder");
    }
}
