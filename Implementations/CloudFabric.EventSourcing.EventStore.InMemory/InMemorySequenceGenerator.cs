using System.Collections.Concurrent;

namespace CloudFabric.EventSourcing.EventStore.InMemory;

public class InMemorySequenceGenerator : ISequenceGenerator
{
    private readonly ConcurrentDictionary<(string SequenceName, string PartitionKey), long> _sequences = new();

    public Task Initialize(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public Task DeleteAll(CancellationToken cancellationToken = default)
    {
        _sequences.Clear();
        return Task.CompletedTask;
    }

    public Task<long> GetNextValue(
        string sequenceName,
        string partitionKey,
        long increment = 1,
        long startingNumber = 1,
        CancellationToken cancellationToken = default
    )
    {
        var result = _sequences.AddOrUpdate(
            (sequenceName, partitionKey),
            startingNumber,
            (_, currentValue) => currentValue + increment
        );

        return Task.FromResult(result);
    }
}
