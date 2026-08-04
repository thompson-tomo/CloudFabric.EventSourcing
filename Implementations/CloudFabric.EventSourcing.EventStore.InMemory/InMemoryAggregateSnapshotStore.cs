using System.Collections.Concurrent;
using CloudFabric.EventSourcing.Domain;

namespace CloudFabric.EventSourcing.EventStore.InMemory;

/// <summary>
/// In-memory snapshot store. Stores the latest snapshot per aggregate stream.
/// Intended for testing; state is lost when the process exits.
/// </summary>
public class InMemoryAggregateSnapshotStore : IAggregateSnapshotStore
{
    private readonly ConcurrentDictionary<(Guid StreamId, string PartitionKey), AggregateSnapshot> _snapshots = new();

    public Task<AggregateSnapshot?> LoadLatestSnapshotAsync(
        Guid streamId,
        string partitionKey,
        CancellationToken cancellationToken = default)
    {
        _snapshots.TryGetValue((streamId, partitionKey), out var snapshot);
        return Task.FromResult(snapshot);
    }

    public Task SaveSnapshotAsync(
        AggregateSnapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        _snapshots[(snapshot.StreamId, snapshot.PartitionKey)] = snapshot;
        return Task.CompletedTask;
    }

    public Task Initialize(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task DeleteAll(CancellationToken cancellationToken = default)
    {
        _snapshots.Clear();
        return Task.CompletedTask;
    }
}
