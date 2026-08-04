using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.Postgresql;

[TestClass]
public class BatchOperationTrackerTestsPostgresql : BatchOperationTrackerTests
{
    private PostgresqlMetadataRepository? _store;

    protected override async Task<IMetadataRepository> GetStore()
    {
        if (_store == null)
        {
            _store = new PostgresqlMetadataRepository(
                TestsConnectionStrings.CONNECTION_STRING,
                "batch_tracker_metadata"
            );
            await _store.Initialize();
        }

        return _store;
    }
}
