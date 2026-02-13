using System.IO.Hashing;
using System.Text;
using CloudFabric.Projections;
using Npgsql;

namespace CloudFabric.Projections.Postgresql;

/// <summary>
/// PostgreSQL advisory lock implementation of <see cref="IDistributedLock"/>.
/// Uses <c>pg_try_advisory_lock</c> (non-blocking) with a hash of the lock key.
/// The lock is session-level: it is held as long as the <see cref="NpgsqlConnection"/> is open.
/// Disposing the returned handle explicitly unlocks and closes the connection.
/// If the process crashes, PostgreSQL automatically releases the lock when the connection drops.
/// </summary>
public class PostgresqlDistributedLock : IDistributedLock
{
    private readonly string _connectionString;

    public PostgresqlDistributedLock(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<IAsyncDisposable?> TryAcquireAsync(string lockKey, CancellationToken cancellationToken = default)
    {
        var lockId = ComputeLockId(lockKey);

        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        try
        {
            await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@lockId)", conn);
            cmd.Parameters.AddWithValue("lockId", lockId);

            var acquired = (bool)(await cmd.ExecuteScalarAsync(cancellationToken))!;

            if (!acquired)
            {
                await conn.DisposeAsync();
                return null;
            }

            return new PostgresqlLockHandle(conn, lockId);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private static long ComputeLockId(string lockKey)
    {
        var bytes = Encoding.UTF8.GetBytes(lockKey);
        return (long)XxHash64.HashToUInt64(bytes);
    }

    private sealed class PostgresqlLockHandle : IAsyncDisposable
    {
        private readonly NpgsqlConnection _conn;
        private readonly long _lockId;

        public PostgresqlLockHandle(NpgsqlConnection conn, long lockId)
        {
            _conn = conn;
            _lockId = lockId;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_conn.State == System.Data.ConnectionState.Open)
                {
                    await using var cmd = new NpgsqlCommand("SELECT pg_advisory_unlock(@lockId)", _conn);
                    cmd.Parameters.AddWithValue("lockId", _lockId);
                    await cmd.ExecuteScalarAsync();
                }
            }
            finally
            {
                await _conn.DisposeAsync();
            }
        }
    }
}
