using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.Postgresql;

[TestClass]
public class SequenceGeneratorTestsPostgresql : SequenceGeneratorTests
{
    private PostgresqlSequenceGenerator? _sequenceGenerator;

    protected override async Task<ISequenceGenerator> GetSequenceGenerator()
    {
        if (_sequenceGenerator == null)
        {
            _sequenceGenerator = new PostgresqlSequenceGenerator(
                TestsConnectionStrings.CONNECTION_STRING,
                "sequence_counters"
            );
            await _sequenceGenerator.Initialize();
        }

        return _sequenceGenerator;
    }
}
