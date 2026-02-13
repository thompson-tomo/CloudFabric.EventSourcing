using System.Collections.Concurrent;
using CloudFabric.Projections;

namespace CloudFabric.Projections.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IDistributedLock"/> for testing.
/// Uses a <see cref="ConcurrentDictionary{TKey,TValue}"/> for lock-free acquire/release.
/// </summary>
public class InMemoryDistributedLock : IDistributedLock
{
    private readonly ConcurrentDictionary<string, byte> _locks = new();

    public Task<IAsyncDisposable?> TryAcquireAsync(string lockKey, CancellationToken cancellationToken = default)
    {
        if (_locks.TryAdd(lockKey, 0))
        {
            return Task.FromResult<IAsyncDisposable?>(new InMemoryLockHandle(_locks, lockKey));
        }

        return Task.FromResult<IAsyncDisposable?>(null);
    }

    private sealed class InMemoryLockHandle : IAsyncDisposable
    {
        private readonly ConcurrentDictionary<string, byte> _locks;
        private readonly string _key;

        public InMemoryLockHandle(ConcurrentDictionary<string, byte> locks, string key)
        {
            _locks = locks;
            _key = key;
        }

        public ValueTask DisposeAsync()
        {
            _locks.TryRemove(_key, out _);
            return ValueTask.CompletedTask;
        }
    }
}
