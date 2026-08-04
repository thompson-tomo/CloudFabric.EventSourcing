using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.InMemory;

[TestClass]
public class SnapshotTestsInMemory : SnapshotTests
{
    private InMemoryEventStore? _eventStore;
    private InMemoryAggregateSnapshotStore? _snapshotStore;

    [TestInitialize]
    public async Task SetUp()
    {
        _eventStore = new InMemoryEventStore(
            new System.Collections.Concurrent.ConcurrentDictionary<(Guid, string), List<string>>()
        );
        await _eventStore.Initialize();
        await _eventStore.DeleteAll();
        _snapshotStore = new InMemoryAggregateSnapshotStore();
    }

    protected override Task<IEventStore> GetEventStore() => Task.FromResult<IEventStore>(_eventStore!);

    protected override IAggregateSnapshotStore GetSnapshotStore() => _snapshotStore!;
}
