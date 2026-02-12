namespace CloudFabric.EventSourcing.EventStore;

public interface ISequenceGenerator
{
    Task Initialize(CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments a named sequence by the given increment and returns the new value.
    /// If the sequence does not exist, it is created starting from <paramref name="startingNumber"/>.
    /// </summary>
    Task<long> GetNextValue(
        string sequenceName,
        string partitionKey,
        long increment = 1,
        long startingNumber = 1,
        CancellationToken cancellationToken = default
    );

    Task DeleteAll(CancellationToken cancellationToken = default);
}
