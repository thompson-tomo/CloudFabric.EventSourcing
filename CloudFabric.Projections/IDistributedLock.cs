namespace CloudFabric.Projections;

/// <summary>
/// Distributed lock abstraction for coordinating projection rebuilds across processes.
/// The lock is identified by a string key (typically the projection index name).
/// </summary>
public interface IDistributedLock
{
    /// <summary>
    /// Attempts to acquire a lock for the given key. Returns an <see cref="IAsyncDisposable"/> handle that
    /// releases the lock when disposed, or <c>null</c> if the lock could not be acquired (another process holds it).
    /// </summary>
    Task<IAsyncDisposable?> TryAcquireAsync(string lockKey, CancellationToken cancellationToken = default);
}
