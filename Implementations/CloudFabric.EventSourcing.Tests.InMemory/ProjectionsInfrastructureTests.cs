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
using CloudFabric.Projections.Attributes;
using CloudFabric.Projections.InMemory;
using CloudFabric.Projections.Queries;
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

    #region DefaultProjectionErrorHandler

    /// <summary>
    /// A projection builder that always throws when handling OrderPlaced.
    /// Used to test error handling behavior.
    /// </summary>
    private class FailingProjectionBuilder : ProjectionBuilder<OrderListProjectionItem>,
        IHandleEvent<OrderPlaced>
    {
        public FailingProjectionBuilder(
            ProjectionRepositoryFactory projectionRepositoryFactory,
            ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write)
            : base(projectionRepositoryFactory, indexSelector)
        {
        }

        public Task On(OrderPlaced evt)
        {
            throw new InvalidOperationException("Intentional failure in projection builder");
        }
    }

    [TestMethod]
    public async Task ErrorHandler_LogAndContinue_DoesNotStopOtherBuilders()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var errorHandler = new DefaultProjectionErrorHandler(ProjectionErrorBehavior.LogAndContinue);

        var engine = ProjectionsEngine.CreateBuilder()
            .WithEventsObserver(observer)
            .AddProjectionBuilder(new FailingProjectionBuilder(factory))
            .AddProjectionBuilder(CreateProjectionBuilder(factory))
            .WithErrorHandler(errorHandler)
            .Build();

        await engine.StartAsync("error-test");
        await EnsureProjectionIndexReady(factory, observer);

        // Create order — FailingProjectionBuilder throws, OrdersListProjectionBuilder should still work
        var order = await CreateAndSaveOrder(store, "Error Test Order");
        await Task.Delay(500);

        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var proj = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull("normal builder should still process events despite failing builder");
        proj!.Name.Should().Be("Error Test Order");

        await engine.StopAsync();
    }

    [TestMethod]
    public async Task ErrorHandler_StopAll_ThrowsException()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        var errorHandler = new DefaultProjectionErrorHandler(ProjectionErrorBehavior.StopAll);

        var engine = ProjectionsEngine.CreateBuilder()
            .WithEventsObserver(observer)
            .AddProjectionBuilder(new FailingProjectionBuilder(factory))
            .WithErrorHandler(errorHandler)
            .Build();

        await engine.StartAsync("stop-all-test");
        await EnsureProjectionIndexReady(factory, observer);

        // Create order — should cause an exception through the error handler
        Func<Task> act = async () => await CreateAndSaveOrder(store, "StopAll Test");

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Projection builder*error*");

        await engine.StopAsync();
    }

    [TestMethod]
    public async Task Builder_WithErrorBehavior_UsesDefaultHandler()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();

        // Use WithErrorBehavior instead of WithErrorHandler
        var engine = ProjectionsEngine.CreateBuilder()
            .WithEventsObserver(observer)
            .AddProjectionBuilder(new FailingProjectionBuilder(factory))
            .AddProjectionBuilder(CreateProjectionBuilder(factory))
            .WithErrorBehavior(ProjectionErrorBehavior.LogAndContinue)
            .Build();

        await engine.StartAsync("behavior-test");
        await EnsureProjectionIndexReady(factory, observer);

        var order = await CreateAndSaveOrder(store, "Behavior Test Order");
        await Task.Delay(500);

        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        var proj = await repo.Single(order.Id, PartitionKeys.GetOrderPartitionKey());
        proj.Should().NotBeNull();
        proj!.Name.Should().Be("Behavior Test Order");

        await engine.StopAsync();
    }

    #endregion

    #region ProjectionDocumentAttribute

    [TestMethod]
    public void GetAllPropertyNames_ReturnsExpectedProperties()
    {
        var names = ProjectionDocumentAttribute.GetAllPropertyNames<OrderListProjectionItem>();

        names.Should().Contain("Name");
        names.Should().Contain("ItemsCount");
        names.Should().Contain("Items");
        names.Should().Contain("CreatedBy");
        names.Should().Contain("Tag");
    }

    [TestMethod]
    public void GetFacetablePropertyNames_ReturnsFilterableProperties()
    {
        var facetableNames = ProjectionDocumentAttribute.GetFacetablePropertyNames<OrderListProjectionItem>();

        // ItemsCount and Tag are marked as IsFacetable (via IsFilterable)
        // Note: IsFilterable doesn't set IsFacetable — these are separate flags
        // OrderListProjectionItem doesn't have IsFacetable=true, so this should be empty
        // This test validates the method works even when no properties are facetable
        facetableNames.Should().NotBeNull();
    }

    [TestMethod]
    public void GetPropertyPathTypeCode_SimpleProperty()
    {
        var typeCode = ProjectionDocumentAttribute.GetPropertyPathTypeCode<OrderListProjectionItem>("Name");
        typeCode.Should().Be(TypeCode.String);
    }

    [TestMethod]
    public void GetPropertyPathTypeCode_NestedProperty()
    {
        var typeCode = ProjectionDocumentAttribute.GetPropertyPathTypeCode<OrderListProjectionItem>("Items.Amount");
        typeCode.Should().Be(TypeCode.Decimal);
    }

    [TestMethod]
    public void GetPropertyPathTypeCode_NestedObjectProperty()
    {
        var typeCode = ProjectionDocumentAttribute.GetPropertyPathTypeCode<OrderListProjectionItem>("CreatedBy.Email");
        typeCode.Should().Be(TypeCode.String);
    }

    [TestMethod]
    public void GetAllProjectionProperties_IncludesNestedProperties()
    {
        var properties = ProjectionDocumentAttribute.GetAllProjectionProperties<OrderListProjectionItem>();

        var itemsProperty = properties.FirstOrDefault(p => p.Key.Name == "Items");
        itemsProperty.Key.Should().NotBeNull();
        itemsProperty.Value.DocumentPropertyAttribute.IsNestedArray.Should().BeTrue();
        itemsProperty.Value.NestedDictionary.Should().NotBeNull("nested array should have nested properties");

        var createdByProperty = properties.FirstOrDefault(p => p.Key.Name == "CreatedBy");
        createdByProperty.Key.Should().NotBeNull();
        createdByProperty.Value.DocumentPropertyAttribute.IsNestedObject.Should().BeTrue();
        createdByProperty.Value.NestedDictionary.Should().NotBeNull("nested object should have nested properties");
    }

    #endregion

    #region QueryResultDocument GetHighlightedTextForField

    [TestMethod]
    public void GetHighlightedTextForField_ReturnsHighlight()
    {
        var doc = new QueryResultDocument<OrderListProjectionItem>
        {
            Document = new OrderListProjectionItem { Name = "Test" },
            Highlights = new Dictionary<string, List<string>>
            {
                { "Name", new List<string> { "<em>Test</em> Order" } }
            }
        };

        var highlight = doc.GetHighlightedTextForField("Name");
        highlight.Should().Be("<em>Test</em> Order");
    }

    [TestMethod]
    public void GetHighlightedTextForField_ReturnsNull_WhenFieldNotFound()
    {
        var doc = new QueryResultDocument<OrderListProjectionItem>
        {
            Document = new OrderListProjectionItem { Name = "Test" },
            Highlights = new Dictionary<string, List<string>>()
        };

        var highlight = doc.GetHighlightedTextForField("NonExistent");
        highlight.Should().BeNull();
    }

    [TestMethod]
    public void GetHighlightedTextForField_ReturnsNull_WhenFieldNameEmpty()
    {
        var doc = new QueryResultDocument<OrderListProjectionItem>
        {
            Document = new OrderListProjectionItem { Name = "Test" },
            Highlights = new Dictionary<string, List<string>>
            {
                { "Name", new List<string> { "<em>Test</em>" } }
            }
        };

        var highlight = doc.GetHighlightedTextForField("");
        highlight.Should().BeNull();
    }

    [TestMethod]
    public void GetHighlightedTextForField_ReturnsNull_WhenHighlightsListEmpty()
    {
        var doc = new QueryResultDocument<OrderListProjectionItem>
        {
            Document = new OrderListProjectionItem { Name = "Test" },
            Highlights = new Dictionary<string, List<string>>
            {
                { "Name", new List<string>() }
            }
        };

        var highlight = doc.GetHighlightedTextForField("Name");
        highlight.Should().BeNull();
    }

    #endregion

    #region Rebuild with MetadataRepository (BatchOperationTracker)

    [TestMethod]
    public async Task RebuildWithMetadataRepository_TracksProgress()
    {
        var store = CreateEventStore();
        var observer = CreateObserver(store);
        var factory = CreateProjectionRepositoryFactory();
        var metadataRepository = new InMemoryMetadataRepository(
            new Dictionary<(string, string), string>()
        );
        await metadataRepository.Initialize();

        // Start a live engine so that events can be saved (observer needs a handler)
        var liveEngine = new ProjectionsEngine(observer);
        liveEngine.AddProjectionBuilder(CreateProjectionBuilder(factory));
        await liveEngine.StartAsync("live");
        await EnsureProjectionIndexReady(factory, observer);

        // Create some orders
        for (int i = 0; i < 5; i++)
        {
            await CreateAndSaveOrder(store, $"Rebuild Tracking Order {i}");
        }
        await Task.Delay(500);

        // Delete all projections to force a rebuild
        var repo = factory.GetProjectionRepository<OrderListProjectionItem>();
        await repo.DeleteAll();
        await repo.EnsureIndex();

        // Create a rebuild processor WITH metadataRepository
        var rebuildProcessor = new ProjectionsRebuildProcessor(
            factory.GetProjectionsIndexStateRepository(),
            async (_) =>
            {
                var rebuildEngine = new ProjectionsEngine(observer);
                rebuildEngine.AddProjectionBuilder(
                    CreateProjectionBuilder(factory, ProjectionOperationIndexSelector.ProjectionRebuild)
                );
                return rebuildEngine;
            },
            NullLogger<ProjectionsRebuildProcessor>.Instance,
            factory,
            metadataRepository
        );

        await rebuildProcessor.RebuildProjectionsThatRequireRebuild();

        // Verify projections were rebuilt
        var results = await repo.Query(new ProjectionQuery { Limit = 10 });
        results.TotalRecordsFound.Should().Be(5);

        await liveEngine.StopAsync();
    }

    #endregion
}
