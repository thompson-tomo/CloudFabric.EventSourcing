using CloudFabric.Projections.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Polly;
using System.Data;
using System.Text.Json;

namespace CloudFabric.EventSourcing.EventStore.Postgresql;

public class PostgresqlMetadataRepository: IMetadataRepository
{
    private readonly PostgresqlEventStoreConnectionInformation _connectionInformation;
    private readonly IPostgresqlEventStoreConnectionInformationProvider? _connectionInformationProvider = null;

    private readonly ResiliencePipeline _retryPipeline;
    private readonly ILogger<PostgresqlMetadataRepository> _logger;

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08001", "08003", "08004", "08006", "40001", "40P01", "57P03", "53300"
    };

    private static bool IsTransientSqlState(string? sqlState) =>
        sqlState != null && TransientSqlStates.Contains(sqlState);

    private PostgresqlEventStoreConnectionInformation ConnectionInformation
    {
        get
        {
            if (_connectionInformationProvider != null)
            {
                return _connectionInformationProvider.GetConnectionInformation();
            }
            else
            {
                return _connectionInformation;
            }
        }
    }

    public PostgresqlMetadataRepository(
        string connectionString,
        string tableName,
        ResilienceSettings? resilienceSettings = null,
        ILogger<PostgresqlMetadataRepository>? logger = null)
    {
        _connectionInformation = new PostgresqlEventStoreConnectionInformation()
        {
            ConnectionString = connectionString,
            MetadataTableName = tableName
        };
        _logger = logger ?? NullLogger<PostgresqlMetadataRepository>.Instance;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default);
    }

    public PostgresqlMetadataRepository(
        IPostgresqlEventStoreConnectionInformationProvider connectionInformationProvider,
        ResilienceSettings? resilienceSettings = null,
        ILogger<PostgresqlMetadataRepository>? logger = null)
    {
        _connectionInformationProvider = connectionInformationProvider;
        _logger = logger ?? NullLogger<PostgresqlMetadataRepository>.Instance;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default);
    }

    private ResiliencePipeline BuildRetryPipeline(ResilienceSettings settings)
    {
        var shouldHandle = new PredicateBuilder<object>()
            .Handle<NpgsqlException>(ex => ex.IsTransient || IsTransientSqlState(ex.SqlState))
            .Handle<TimeoutException>();

        return ResiliencePipelineFactory.Create(settings, shouldHandle, _logger, "PostgresqlMetadataRepository");
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await EnsureTableExistsAsync(ct);
        }, cancellationToken);
    }

    public async Task DeleteAll(CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            var connectionInformation = ConnectionInformation;

            await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
            await conn.OpenAsync(ct);

            await using var itemsTableCmd = new NpgsqlCommand($"DELETE FROM \"{connectionInformation.MetadataTableName}\"", conn);

            try
            {
                await itemsTableCmd.ExecuteScalarAsync(ct);
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState != PostgresErrorCodes.UndefinedTable)
                {
                    throw;
                }
            }
        }, cancellationToken);
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT 1 FROM \"{connectionInformation.MetadataTableName}\"", conn)
        {
        };

        try
        {
            await cmd.ExecuteScalarAsync(cancellationToken);
        }
        catch (NpgsqlException ex)
        {
            if (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                await using var createTableCommand = new NpgsqlCommand(
                    $"CREATE TABLE \"{connectionInformation.MetadataTableName}\" (" +
                    $"id varchar(100) UNIQUE NOT NULL, " +
                    $"partition_key varchar(100) NOT NULL, " +
                    $"data jsonb" +
                    $");" +
                    $"CREATE INDEX \"{connectionInformation.MetadataTableName}_id_idx\" ON \"{connectionInformation.MetadataTableName}\" (id);" +
                    $"CREATE INDEX \"{connectionInformation.MetadataTableName}_id_with_partition_key_idx\" ON \"{connectionInformation.MetadataTableName}\" (id, partition_key);"
                    , conn);

                await createTableCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    public async Task UpsertItem<T>(string id, string partitionKey, T item, CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            var connectionInformation = ConnectionInformation;

            await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO \"{connectionInformation.MetadataTableName}\" " +
                $"(id, partition_key, data) " +
                $"VALUES" +
                $"(@id, @partition_key, @data)" +
                $"ON CONFLICT (id) " +
                $"DO UPDATE " +
                $"SET data = @data, partition_key = @partition_key; "
                , conn
            )
            {
                Parameters =
                {
                    new("id", id),
                    new("partition_key", partitionKey),
                    new NpgsqlParameter()
                    {
                        ParameterName = "data",
                        Value = JsonSerializer.Serialize(item, EventStoreSerializerOptions.Options),
                        DataTypeName = "jsonb"
                    }
                }
            };

            try
            {
                int insertItemResult = await cmd.ExecuteNonQueryAsync(ct);

                if (insertItemResult == -1)
                {
                    throw new Exception("Upsert item failed.");
                }
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    throw new Exception(
                        "EventStore table not found, please make sure to call Initialize() on event store first.",
                        ex);
                }

                throw;
            }
        }, cancellationToken);
    }

    public async Task<T?> LoadItem<T>(string id, string partitionKey, CancellationToken cancellationToken = default)
    {
        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            var connectionInformation = ConnectionInformation;

            await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"SELECT * FROM \"{connectionInformation.MetadataTableName}\" " +
                $"WHERE id = @id AND partition_key = @partition_key LIMIT 1; "
                , conn)
            {
                Parameters =
                    {
                        new("id", id),
                        new("partition_key", partitionKey)
                    }
            };

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (await reader.ReadAsync(ct))
                {
                    var item = JsonDocument.Parse(reader.GetString("data")).RootElement;

                    return JsonSerializer.Deserialize<T>(item, EventStoreSerializerOptions.Options);
                }

                return default;
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    throw new Exception(
                        "EventStore table not found, please make sure to call Initialize() on event store first.",
                        ex);
                }

                throw;
            }
        }, cancellationToken);
    }
}