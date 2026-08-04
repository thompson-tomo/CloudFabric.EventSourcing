using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.Postgresql;

[TestClass]
public class SnapshotTestsPostgresql : SnapshotTests
{
    private PostgresqlEventStore? _eventStore;
    private PostgresqlAggregateSnapshotStore? _snapshotStore;

    [TestInitialize]
    public async Task SetUp()
    {
        _eventStore = new PostgresqlEventStore(
            TestsConnectionStrings.CONNECTION_STRING,
            "snapshot_tests_events",
            "snapshot_tests_meta"
        );
        await _eventStore.Initialize();
        await _eventStore.DeleteAll();

        _snapshotStore = new PostgresqlAggregateSnapshotStore(
            TestsConnectionStrings.CONNECTION_STRING,
            "snapshot_tests_snapshots"
        );
        await _snapshotStore.Initialize();
        await _snapshotStore.DeleteAll();
    }

    protected override Task<IEventStore> GetEventStore() => Task.FromResult<IEventStore>(_eventStore!);

    protected override IAggregateSnapshotStore GetSnapshotStore() => _snapshotStore!;
}
