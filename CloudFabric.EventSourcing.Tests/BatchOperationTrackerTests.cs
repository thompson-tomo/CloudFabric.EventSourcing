using CloudFabric.EventSourcing.EventStore;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests;

public abstract class BatchOperationTrackerTests
{
    protected abstract Task<IMetadataRepository> GetStore();
    private IMetadataRepository _metadataRepository;

    [TestInitialize]
    public async Task Initialize()
    {
        _metadataRepository = await GetStore();
    }

    [TestCleanup]
    public async Task Cleanup()
    {
        await _metadataRepository.DeleteAll();
    }

    [TestMethod]
    public async Task StartAsync_CreatesTrackerWithCorrectInitialState()
    {
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "test-operation", totalItems: 100);

        tracker.State.Should().NotBeNull();
        tracker.State.OperationType.Should().Be("test-operation");
        tracker.State.TotalItems.Should().Be(100);
        tracker.State.ProcessedItems.Should().Be(0);
        tracker.State.Status.Should().Be("in_progress");
        tracker.State.StartedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        tracker.State.CompletedAt.Should().BeNull();
        tracker.State.ErrorMessage.Should().BeNull();
    }

    [TestMethod]
    public async Task UpdateProgressAsync_UpdatesProcessedItems()
    {
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "test-operation", totalItems: 100);

        await tracker.UpdateProgressAsync(25);
        tracker.State.ProcessedItems.Should().Be(25);

        await tracker.UpdateProgressAsync(50);
        tracker.State.ProcessedItems.Should().Be(50);

        // Verify persisted state
        var loaded = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker.State.OperationId);
        loaded.Should().NotBeNull();
        loaded!.ProcessedItems.Should().Be(50);
        loaded.Status.Should().Be("in_progress");
    }

    [TestMethod]
    public async Task UpdateProgressAsync_WithMetadata_PersistsMetadata()
    {
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "test-operation", totalItems: 100);

        var metadata = new Dictionary<string, object?>
        {
            ["projectionName"] = "orders",
            ["indexName"] = "orders_v2",
            ["isComplete"] = false
        };

        await tracker.UpdateProgressAsync(50, metadata);
        tracker.State.ProcessedItems.Should().Be(50);
        tracker.State.Metadata.Should().NotBeNull();
        tracker.State.Metadata!["projectionName"].Should().Be("orders");

        // Verify persisted
        var loaded = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker.State.OperationId);
        loaded.Should().NotBeNull();
        loaded!.Metadata.Should().NotBeNull();
        loaded.Metadata!["projectionName"]!.ToString().Should().Be("orders");
        loaded.Metadata["indexName"]!.ToString().Should().Be("orders_v2");
    }

    [TestMethod]
    public async Task CompleteAsync_SetsCompletedStatus()
    {
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "test-operation", totalItems: 100);

        await tracker.UpdateProgressAsync(100);
        await tracker.CompleteAsync();

        tracker.State.Status.Should().Be("completed");
        tracker.State.CompletedAt.Should().NotBeNull();
        tracker.State.CompletedAt!.Value.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));

        // Verify persisted
        var loaded = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker.State.OperationId);
        loaded.Should().NotBeNull();
        loaded!.Status.Should().Be("completed");
        loaded.CompletedAt.Should().NotBeNull();
    }

    [TestMethod]
    public async Task FailAsync_SetsFailedStatusWithErrorMessage()
    {
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "test-operation", totalItems: 100);

        await tracker.UpdateProgressAsync(30);
        await tracker.FailAsync("Connection refused");

        tracker.State.Status.Should().Be("failed");
        tracker.State.ErrorMessage.Should().Be("Connection refused");
        tracker.State.ProcessedItems.Should().Be(30);

        // Verify persisted
        var loaded = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker.State.OperationId);
        loaded.Should().NotBeNull();
        loaded!.Status.Should().Be("failed");
        loaded.ErrorMessage.Should().Be("Connection refused");
    }

    [TestMethod]
    public async Task LoadAsync_ReturnsNullForNonExistentOperation()
    {
        var loaded = await BatchOperationTracker.LoadAsync(
            _metadataRepository, "non-existent-id");

        loaded.Should().BeNull();
    }

    [TestMethod]
    public async Task StartAsync_WithPartitionKey_IsolatesOperations()
    {
        var tracker1 = await BatchOperationTracker.StartAsync(
            _metadataRepository, "import", totalItems: 50, "tenant-1");

        var tracker2 = await BatchOperationTracker.StartAsync(
            _metadataRepository, "import", totalItems: 75, "tenant-2");

        await tracker1.UpdateProgressAsync(25);
        await tracker2.UpdateProgressAsync(50);

        // Load with correct partition key
        var loaded1 = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker1.State.OperationId, "tenant-1");
        loaded1.Should().NotBeNull();
        loaded1!.ProcessedItems.Should().Be(25);
        loaded1.TotalItems.Should().Be(50);

        var loaded2 = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker2.State.OperationId, "tenant-2");
        loaded2.Should().NotBeNull();
        loaded2!.ProcessedItems.Should().Be(50);
        loaded2.TotalItems.Should().Be(75);

        // Load with wrong partition key returns null
        var wrongPartition = await BatchOperationTracker.LoadAsync(
            _metadataRepository, tracker1.State.OperationId, "tenant-2");
        wrongPartition.Should().BeNull();
    }

    [TestMethod]
    public async Task FullLifecycle_StartUpdateCompleteLoad()
    {
        // Start
        var tracker = await BatchOperationTracker.StartAsync(
            _metadataRepository, "projection-rebuild", totalItems: 1000);
        var operationId = tracker.State.OperationId;

        // Update progress in chunks
        for (int i = 250; i <= 1000; i += 250)
        {
            await tracker.UpdateProgressAsync(i, new Dictionary<string, object?>
            {
                ["lastChunk"] = i
            });
        }

        // Complete
        await tracker.CompleteAsync();

        // Load and verify final state
        var finalState = await BatchOperationTracker.LoadAsync(
            _metadataRepository, operationId);

        finalState.Should().NotBeNull();
        finalState!.OperationType.Should().Be("projection-rebuild");
        finalState.TotalItems.Should().Be(1000);
        finalState.ProcessedItems.Should().Be(1000);
        finalState.Status.Should().Be("completed");
        finalState.CompletedAt.Should().NotBeNull();
        finalState.ErrorMessage.Should().BeNull();
    }
}
