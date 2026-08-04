namespace CloudFabric.EventSourcing.Domain;

/// <summary>
/// Persistence contract for aggregate snapshots.
/// Implementations store at most one snapshot per (StreamId, PartitionKey) pair;
/// saving a new snapshot replaces the previous one (upsert semantics).
/// </summary>
public interface IAggregateSnapshotStore
{
    /// <summary>Returns the latest snapshot for the given aggregate stream, or null if none exists.</summary>
    Task<AggregateSnapshot?> LoadLatestSnapshotAsync(
        Guid streamId,
        string partitionKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>Persists a snapshot, replacing any existing snapshot for the same stream.</summary>
    Task SaveSnapshotAsync(
        AggregateSnapshot snapshot,
        CancellationToken cancellationToken = default
    );

    /// <summary>Creates the underlying storage if it does not already exist.</summary>
    Task Initialize(CancellationToken cancellationToken = default);

    /// <summary>Deletes all snapshots. Intended for tests and maintenance only.</summary>
    Task DeleteAll(CancellationToken cancellationToken = default);
}
