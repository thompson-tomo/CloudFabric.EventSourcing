using CloudFabric.Projections.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Polly;

namespace CloudFabric.EventSourcing.EventStore.Postgresql;

public class PostgresqlSequenceGenerator : ISequenceGenerator
{
    private readonly PostgresqlEventStoreConnectionInformation _connectionInformation;
    private readonly IPostgresqlEventStoreConnectionInformationProvider? _connectionInformationProvider = null;
    private readonly string _tableName;

    private readonly ResiliencePipeline _retryPipeline;
    private readonly ILogger<PostgresqlSequenceGenerator> _logger;

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

    public PostgresqlSequenceGenerator(
        string connectionString,
        string tableName,
        ResilienceSettings? resilienceSettings = null,
        ILogger<PostgresqlSequenceGenerator>? logger = null)
    {
        _tableName = tableName;
        _connectionInformation = new PostgresqlEventStoreConnectionInformation()
        {
            ConnectionString = connectionString
        };
        _logger = logger ?? NullLogger<PostgresqlSequenceGenerator>.Instance;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default);
    }

    public PostgresqlSequenceGenerator(
        IPostgresqlEventStoreConnectionInformationProvider connectionInformationProvider,
        string tableName = "sequence_counters",
        ResilienceSettings? resilienceSettings = null,
        ILogger<PostgresqlSequenceGenerator>? logger = null
    )
    {
        _tableName = tableName;
        _connectionInformationProvider = connectionInformationProvider;
        _logger = logger ?? NullLogger<PostgresqlSequenceGenerator>.Instance;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default);
    }

    private ResiliencePipeline BuildRetryPipeline(ResilienceSettings settings)
    {
        var shouldHandle = new PredicateBuilder<object>()
            .Handle<NpgsqlException>(ex => ex.IsTransient || IsTransientSqlState(ex.SqlState))
            .Handle<TimeoutException>();

        return ResiliencePipelineFactory.Create(settings, shouldHandle, _logger, "PostgresqlSequenceGenerator");
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

            await using var cmd = new NpgsqlCommand($"DELETE FROM \"{_tableName}\"", conn);

            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
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

    public async Task<long> GetNextValue(
        string sequenceName,
        string partitionKey,
        long increment = 1,
        long startingNumber = 1,
        CancellationToken cancellationToken = default
    )
    {
        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            var connectionInformation = ConnectionInformation;

            await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO \"{_tableName}\" (sequence_name, partition_key, current_value) " +
                $"VALUES (@sequence_name, @partition_key, @starting_number) " +
                $"ON CONFLICT (sequence_name, partition_key) " +
                $"DO UPDATE SET current_value = \"{_tableName}\".current_value + @increment " +
                $"RETURNING current_value",
                conn
            )
            {
                Parameters =
                {
                    new("sequence_name", sequenceName),
                    new("partition_key", partitionKey),
                    new("starting_number", startingNumber),
                    new("increment", increment)
                }
            };

            try
            {
                var result = await cmd.ExecuteScalarAsync(ct);
                return (long)result!;
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    throw new Exception(
                        "Sequence table not found, please make sure to call Initialize() on sequence generator first.",
                        ex
                    );
                }

                throw;
            }
        }, cancellationToken);
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT 1 FROM \"{_tableName}\"", conn)
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
                    $"CREATE TABLE \"{_tableName}\" (" +
                    $"sequence_name VARCHAR(256) NOT NULL, " +
                    $"partition_key VARCHAR(256) NOT NULL, " +
                    $"current_value BIGINT NOT NULL, " +
                    $"PRIMARY KEY (sequence_name, partition_key)" +
                    $")",
                    conn
                );

                await createTableCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }
}
