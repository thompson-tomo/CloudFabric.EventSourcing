using CloudFabric.EventSourcing.Domain;
using CloudFabric.Projections.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Polly;

namespace CloudFabric.EventSourcing.EventStore.Postgresql;

/// <summary>
/// PostgreSQL-backed aggregate snapshot store.
/// One snapshot per (stream_id, partition_key) — upsert on save.
/// Schema: see <see cref="EnsureTableExistsAsync"/>.
/// </summary>
public class PostgresqlAggregateSnapshotStore : IAggregateSnapshotStore
{
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly ILogger<PostgresqlAggregateSnapshotStore> _logger;

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08001", "08003", "08004", "08006", "40001", "40P01", "57P03", "53300"
    };

    private static bool IsTransientSqlState(string? sqlState) =>
        sqlState != null && TransientSqlStates.Contains(sqlState);

    public PostgresqlAggregateSnapshotStore(
        string connectionString,
        string tableName = "aggregate_snapshots",
        ResilienceSettings? resilienceSettings = null,
        ILogger<PostgresqlAggregateSnapshotStore>? logger = null)
    {
        _connectionString = connectionString;
        _tableName = tableName;
        _logger = logger ?? NullLogger<PostgresqlAggregateSnapshotStore>.Instance;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default);
    }

    private ResiliencePipeline BuildRetryPipeline(ResilienceSettings settings)
    {
        var shouldHandle = new PredicateBuilder<object>()
            .Handle<NpgsqlException>(ex => ex.IsTransient || IsTransientSqlState(ex.SqlState))
            .Handle<TimeoutException>();

        return ResiliencePipelineFactory.Create(settings, shouldHandle, _logger, "PostgresqlAggregateSnapshotStore");
    }

    /// <inheritdoc />
    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async ct =>
        {
            await EnsureTableExistsAsync(ct);
        }, cancellationToken);
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Probe the table — create if it doesn't exist.
        await using var probe = new NpgsqlCommand(
            $"SELECT 1 FROM \"{_tableName}\" LIMIT 1", conn);

        try
        {
            await probe.ExecuteScalarAsync(cancellationToken);
        }
        catch (NpgsqlException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            await using var create = new NpgsqlCommand(
                $"""
                CREATE TABLE "{_tableName}" (
                    stream_id                     uuid          NOT NULL,
                    partition_key                 varchar(500)  NOT NULL,
                    aggregate_type                varchar(1000) NOT NULL,
                    stream_version                integer       NOT NULL,
                    state_data                    text          NOT NULL,
                    last_applied_event_timestamp  timestamp     NOT NULL,
                    created_at                    timestamp     NOT NULL,
                    PRIMARY KEY (stream_id, partition_key)
                );
                """, conn);

            await create.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <inheritdoc />
    public async Task<AggregateSnapshot?> LoadLatestSnapshotAsync(
        Guid streamId,
        string partitionKey,
        CancellationToken cancellationToken = default)
    {
        return await _retryPipeline.ExecuteAsync(async ct =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"""
                SELECT stream_id, partition_key, aggregate_type, stream_version,
                       state_data, last_applied_event_timestamp, created_at
                FROM   "{_tableName}"
                WHERE  stream_id = @stream_id AND partition_key = @partition_key
                LIMIT  1
                """, conn)
            {
                Parameters =
                {
                    new("stream_id",      streamId),
                    new("partition_key",  partitionKey)
                }
            };

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (await reader.ReadAsync(ct))
                {
                    return new AggregateSnapshot
                    {
                        StreamId                   = reader.GetGuid(0),
                        PartitionKey               = reader.GetString(1),
                        AggregateType              = reader.GetString(2),
                        Version                    = reader.GetInt32(3),
                        StateJson                  = reader.GetString(4),
                        LastAppliedEventTimestamp  = DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc),
                    };
                }

                return null;
            }
            catch (NpgsqlException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                return null;
            }
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task SaveSnapshotAsync(
        AggregateSnapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async ct =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"""
                INSERT INTO "{_tableName}"
                    (stream_id, partition_key, aggregate_type, stream_version,
                     state_data, last_applied_event_timestamp, created_at)
                VALUES
                    (@stream_id, @partition_key, @aggregate_type, @stream_version,
                     @state_data, @last_applied_event_timestamp, @created_at)
                ON CONFLICT (stream_id, partition_key)
                DO UPDATE SET
                    aggregate_type               = EXCLUDED.aggregate_type,
                    stream_version               = EXCLUDED.stream_version,
                    state_data                   = EXCLUDED.state_data,
                    last_applied_event_timestamp = EXCLUDED.last_applied_event_timestamp,
                    created_at                   = EXCLUDED.created_at
                """, conn)
            {
                Parameters =
                {
                    new("stream_id",                    snapshot.StreamId),
                    new("partition_key",                snapshot.PartitionKey),
                    new("aggregate_type",               snapshot.AggregateType),
                    new("stream_version",               snapshot.Version),
                    new("state_data",                   snapshot.StateJson),
                    new("last_applied_event_timestamp", snapshot.LastAppliedEventTimestamp.ToUniversalTime()),
                    new("created_at",                   DateTime.UtcNow)
                }
            };

            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (NpgsqlException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                throw new InvalidOperationException(
                    "Snapshot table not found. Call Initialize() before using the snapshot store.", ex);
            }
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteAll(CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async ct =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand($"DELETE FROM \"{_tableName}\"", conn);

            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (NpgsqlException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                // Table does not exist yet — nothing to delete.
            }
        }, cancellationToken);
    }
}
