using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Persistence;
using CloudFabric.EventSourcing.Tests.Domain;
using CloudFabric.EventSourcing.Tests.Domain.Events;
using CloudFabric.EventSourcing.Tests.Domain.ValueObjects;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests;

/// <summary>
/// Tests for aggregate snapshot save/restore mechanics.
/// Uses threshold=5: a snapshot is persisted whenever an aggregate's version is a multiple of 5.
/// </summary>
public abstract class SnapshotTests
{
    private const int SnapshotThreshold = 5;

    protected abstract Task<IEventStore> GetEventStore();
    protected abstract IAggregateSnapshotStore GetSnapshotStore();

    private AggregateRepository<Order> BuildRepository(IEventStore store) =>
        new(store, GetSnapshotStore(), SnapshotThreshold);

    // ── helpers ──────────────────────────────────────────────────────────────

    private static EventUserInfo UserInfo() => new(Guid.NewGuid());

    private static Order CreateOrder(Guid id, string name) =>
        new(id, name, new List<OrderItem>(), Guid.NewGuid(), "test@example.com");

    /// <summary>
    /// Saves <paramref name="count"/> OrderNameUpdated events one by one so each
    /// SaveAsync triggers the version counter.
    /// </summary>
    private static async Task RenameOrder(
        AggregateRepository<Order> repo, IEventStore store,
        Guid id, string partitionKey, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var o = await repo.LoadAsync(id, partitionKey);
            o!.Ship($"tracking-{i}"); // Ship emits OrderShipped — any mutation works
            await repo.SaveAsync(UserInfo(), o);
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// After exactly <see cref="SnapshotThreshold"/> own events, a snapshot must be created.
    /// </summary>
    [TestMethod]
    public async Task TestSnapshotCreatedAtThreshold()
    {
        var store = await GetEventStore();
        var repo = BuildRepository(store);
        var snapshotStore = GetSnapshotStore();

        var id = Guid.NewGuid();
        var partitionKey = PartitionKeys.GetOrderPartitionKey();

        // Event 1: OrderPlaced
        var order = CreateOrder(id, "Original");
        await repo.SaveAsync(UserInfo(), order);

        // Events 2–5: Ship × 4 (gives us 4 more events = 5 total at v5)
        await RenameOrder(repo, store, id, partitionKey, SnapshotThreshold - 1);

        // After version == SnapshotThreshold (= 5), a snapshot must exist.
        var snapshot = await snapshotStore.LoadLatestSnapshotAsync(id, partitionKey);
        snapshot.Should().NotBeNull("a snapshot should have been persisted at version {0}", SnapshotThreshold);
        snapshot!.Version.Should().Be(SnapshotThreshold);
    }

    /// <summary>
    /// An aggregate loaded after snapshot creation must have the same state as
    /// the one that triggered the snapshot save.
    /// </summary>
    [TestMethod]
    public async Task TestLoadFromSnapshot_RestoresCorrectState()
    {
        var store = await GetEventStore();
        var repo = BuildRepository(store);

        var id = Guid.NewGuid();
        var partitionKey = PartitionKeys.GetOrderPartitionKey();
        var originalName = "Snapshot Load Test";

        var order = CreateOrder(id, originalName);
        await repo.SaveAsync(UserInfo(), order);

        // Reach threshold
        await RenameOrder(repo, store, id, partitionKey, SnapshotThreshold - 1);

        // Load — this should use the snapshot path.
        var loaded = await repo.LoadAsync(id, partitionKey);

        loaded.Should().NotBeNull();
        loaded!.Id.Should().Be(id);
        loaded.OrderName.Should().Be(originalName);
        loaded.Version.Should().Be(SnapshotThreshold);
    }

    /// <summary>
    /// Events that occur AFTER the snapshot must still be replayed on top of the
    /// restored snapshot state.
    /// </summary>
    [TestMethod]
    public async Task TestLoadFromSnapshot_RemainingEventsApplied()
    {
        var store = await GetEventStore();
        var repo = BuildRepository(store);

        var id = Guid.NewGuid();
        var partitionKey = PartitionKeys.GetOrderPartitionKey();

        var order = CreateOrder(id, "Initial Name");
        await repo.SaveAsync(UserInfo(), order);

        // Reach threshold (version 5, snapshot saved)
        await RenameOrder(repo, store, id, partitionKey, SnapshotThreshold - 1);

        // One more event AFTER the snapshot
        var postSnapshot = await repo.LoadAsync(id, partitionKey);
        postSnapshot!.Ship("tracking-after-snapshot");
        await repo.SaveAsync(UserInfo(), postSnapshot);

        // Load — must be at version 6 with the post-snapshot tracking number
        var loaded = await repo.LoadAsync(id, partitionKey);

        loaded.Should().NotBeNull();
        loaded!.Version.Should().Be(SnapshotThreshold + 1);
        loaded.TrackingNumber.Should().Be("tracking-after-snapshot");
    }

    /// <summary>
    /// A cross-aggregate event (BulkOrderTagChanged) emitted AFTER a snapshot has been
    /// saved must be replayed when loading from the snapshot, because its timestamp is
    /// strictly greater than <see cref="AggregateSnapshot.LastAppliedEventTimestamp"/>.
    /// </summary>
    [TestMethod]
    public async Task TestLoadFromSnapshot_CrossAggregateEventAfterSnapshotIsApplied()
    {
        var store = await GetEventStore();
        var repo = BuildRepository(store);

        var id = Guid.NewGuid();
        var partitionKey = PartitionKeys.GetOrderPartitionKey();

        var order = CreateOrder(id, "Cross-Aggregate Test");
        await repo.SaveAsync(UserInfo(), order);

        // Reach threshold — snapshot is saved here.
        await RenameOrder(repo, store, id, partitionKey, SnapshotThreshold - 1);

        // Small delay to guarantee the global event gets a strictly later timestamp.
        await Task.Delay(15);

        // Emit a global event AFTER the snapshot timestamp.
        var globalEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "POST_SNAPSHOT_TAG",
            partitionKey
        );
        await store.AppendGlobalEventAsync(UserInfo(), globalEvent);

        // Load — cross-aggregate event must be applied on top of snapshot state.
        var loaded = await repo.LoadAsync(id, partitionKey);

        loaded.Should().NotBeNull();
        loaded!.Tag.Should().Be("POST_SNAPSHOT_TAG",
            "global event after snapshot must be replayed");
    }

    /// <summary>
    /// A cross-aggregate event emitted BEFORE a snapshot must NOT be re-applied
    /// when loading from the snapshot (it is already captured in the snapshot state).
    /// </summary>
    [TestMethod]
    public async Task TestLoadFromSnapshot_CrossAggregateEventBeforeSnapshotNotReapplied()
    {
        var store = await GetEventStore();
        var repo = BuildRepository(store);

        var id = Guid.NewGuid();
        var partitionKey = PartitionKeys.GetOrderPartitionKey();

        var order = CreateOrder(id, "Pre-Snapshot Global Event Test");
        await repo.SaveAsync(UserInfo(), order);

        // Emit a global event BEFORE we reach the snapshot threshold.
        var globalEvent = new BulkOrderTagChanged(
            typeof(Order).AssemblyQualifiedName!,
            "BEFORE_SNAPSHOT",
            partitionKey
        );
        await store.AppendGlobalEventAsync(UserInfo(), globalEvent);

        // Verify the global event is visible via full event replay.
        var beforeSnapshot = await repo.LoadAsync(id, partitionKey);
        beforeSnapshot!.Tag.Should().Be("BEFORE_SNAPSHOT");

        // Small delay so the remaining events have timestamps after the global event.
        await Task.Delay(15);

        // Reach snapshot threshold — the snapshot captures Tag="BEFORE_SNAPSHOT".
        await RenameOrder(repo, store, id, partitionKey, SnapshotThreshold - 1);

        var snapshotStore = GetSnapshotStore();
        var snapshot = await snapshotStore.LoadLatestSnapshotAsync(id, partitionKey);
        snapshot.Should().NotBeNull();

        // Load from snapshot — Tag must still be "BEFORE_SNAPSHOT", not ""
        // (the global event must not be re-applied, causing double-application bugs).
        var loaded = await repo.LoadAsync(id, partitionKey);
        loaded!.Tag.Should().Be("BEFORE_SNAPSHOT",
            "global event captured in snapshot must not be re-applied");
    }
}
