using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.InMemory;

[TestClass]
public class BatchOperationTrackerTestsInMemory : BatchOperationTrackerTests
{
    private InMemoryMetadataRepository? _store = null;

    protected override async Task<IMetadataRepository> GetStore()
    {
        if (_store == null)
        {
            _store = new InMemoryMetadataRepository(
                new Dictionary<(string, string), string>()
            );
            await _store.Initialize();
        }

        return _store;
    }
}
