using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.InMemory;

[TestClass]
public class SequenceGeneratorTestInMemory : SequenceGeneratorTests
{
    private InMemorySequenceGenerator? _sequenceGenerator = null;

    protected override async Task<ISequenceGenerator> GetSequenceGenerator()
    {
        if (_sequenceGenerator == null)
        {
            _sequenceGenerator = new InMemorySequenceGenerator();
            await _sequenceGenerator.Initialize();
        }

        return _sequenceGenerator;
    }
}
