using System.Data;
using System.Text.Json;
using CloudFabric.EventSourcing.EventStore.Persistence;
using Npgsql;

namespace CloudFabric.EventSourcing.EventStore.Postgresql;

public class PostgresqlEventStore : IEventStore
{
    private const int EVENTSTORE_TABLE_SCHEMA_VERSION = 1;
    
    private readonly List<Func<IEvent, Task>> _eventAddedEventHandlers = new();
    
    private readonly PostgresqlEventStoreConnectionInformation _connectionInformation;
    private readonly IPostgresqlEventStoreConnectionInformationProvider? _connectionInformationProvider = null;
    
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

    public PostgresqlEventStore(string connectionString, string eventsTableName, string itemsTableName)
    {
        _connectionInformation = new PostgresqlEventStoreConnectionInformation()
        {
            ConnectionString = connectionString,
            TableName = eventsTableName,
            MetadataTableName = itemsTableName
        };
    }

    public PostgresqlEventStore(IPostgresqlEventStoreConnectionInformationProvider connectionInformationProvider)
    {
        _connectionInformationProvider = connectionInformationProvider;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        await EnsureTableExistsAsync(cancellationToken);
    }

    
    public async Task<EventStoreStatistics> GetStatistics(CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;
        var stats = new EventStoreStatistics();

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync();

        await using var countCmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM \"{connectionInformation.TableName}\"", conn
        );
        await using var firstEventTimestampCmd = new NpgsqlCommand(
            $"SELECT created_at " +
            $"FROM \"{connectionInformation.TableName}\" " +
            $"ORDER BY created_at ASC " +
            $"LIMIT 1", conn
        );
        await using var lastEventTimestampCmd = new NpgsqlCommand(
            $"SELECT created_at " +
            $"FROM \"{connectionInformation.TableName}\" " +
            $"ORDER BY created_at DESC " +
            $"LIMIT 1", conn
        );
    
        var count = await countCmd.ExecuteScalarAsync(cancellationToken);

        if (count != null)
        {
            stats.TotalEventsCount = (long)count;
        }
        
        var firstEventDateTime = await firstEventTimestampCmd.ExecuteScalarAsync(cancellationToken);

        if (firstEventDateTime != null)
        {
            stats.FirstEventCreatedAt = (DateTime)firstEventDateTime;
        }
        
        var lastEventDateTime = await lastEventTimestampCmd.ExecuteScalarAsync(cancellationToken);

        if (lastEventDateTime != null)
        {
            stats.LastEventCreatedAt = (DateTime)lastEventDateTime;
        }

        return stats;
    }
    
    public async Task DeleteAll(CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;
        
        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync();

        await using var eventsTableCmd = new NpgsqlCommand($"DELETE FROM \"{connectionInformation.TableName}\"", conn);
        
        try
        {
            await eventsTableCmd.ExecuteScalarAsync(cancellationToken);
        }
        catch (NpgsqlException ex)
        {
            if (ex.SqlState != PostgresErrorCodes.UndefinedTable)
            {
                throw;
            }
        }

        await using var itemsTableCmd = new NpgsqlCommand($"DELETE FROM \"{connectionInformation.MetadataTableName}\"", conn);

        try
        {
            await itemsTableCmd.ExecuteScalarAsync(cancellationToken);
        }
        catch (NpgsqlException ex)
        {
            if (ex.SqlState != PostgresErrorCodes.UndefinedTable)
            {
                throw;
            }
        }

    }
    public async Task<bool> HardDeleteAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;
        
        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync();

        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"DELETE FROM \"{connectionInformation.TableName}\"" +
            $"WHERE stream_id = @streamId AND partition_key = @partitionKey",
            conn,
            transaction)
        {
            Parameters =
            {
                new("streamId", streamId),
                new("partitionKey", partitionKey)
            }
        };

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
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
        
        await transaction.CommitAsync(cancellationToken);

        return true;
    }

    public async Task<EventStream> LoadStreamAsyncOrThrowNotFound(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var eventStream = await LoadStreamAsync(streamId, partitionKey, cancellationToken);

        if (!eventStream.Events.Any())
        {
            throw new NotFoundException();
        }

        return eventStream;
    }

    public async Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;
        
        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT id, stream_id, stream_version, event_type, event_data, user_info " +
            $"FROM \"{connectionInformation.TableName}\" " +
            $"WHERE stream_id = @streamId AND partition_key = @partitionKey ORDER BY stream_version ASC", conn)
        {
            Parameters =
            {
                new("streamId", streamId),
                new("partitionKey", partitionKey)
            }
        };

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var version = 0;
            var events = new List<IEvent>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var eventWrapper = new EventWrapper()
                {
                    Id = reader.GetGuid("id"),
                    StreamInfo = new StreamInfo
                    {
                        Id = reader.GetGuid("stream_id"),
                        Version = reader.GetInt16("stream_version")
                    },
                    EventType = reader.GetString("event_type"),
                    EventData = JsonSerializer.Deserialize<JsonElement>(reader.GetString("event_data")),
                    UserInfo = JsonSerializer.Deserialize<JsonElement>(reader.GetString("user_info")),
                };

                version = eventWrapper.StreamInfo.Version;

                events.Add(eventWrapper.GetEvent());
            }

            return new EventStream(streamId, version, events);
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
    }

    public async Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, int fromVersion, CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT id, stream_id, stream_version, event_type, event_data, user_info " +
            $"FROM \"{connectionInformation.TableName}\" " +
            $"WHERE stream_id = @streamId AND partition_key = @partitionKey AND stream_version >= @fromVersion ORDER BY stream_version ASC", conn)
        {
            Parameters =
            {
                new("streamId", streamId),
                new("partitionKey", partitionKey),
                new("fromVersion", fromVersion)
            }
        };

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var version = 0;
            var events = new List<IEvent>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var eventWrapper = new EventWrapper()
                {
                    Id = reader.GetGuid("id"),
                    StreamInfo = new StreamInfo
                    {
                        Id = reader.GetGuid("stream_id"),
                        Version = reader.GetInt16("stream_version")
                    },
                    EventType = reader.GetString("event_type"),
                    EventData = JsonSerializer.Deserialize<JsonElement>(reader.GetString("event_data")),
                    UserInfo = JsonSerializer.Deserialize<JsonElement>(reader.GetString("user_info")),
                };

                version = eventWrapper.StreamInfo.Version;

                events.Add(eventWrapper.GetEvent());
            }

            return new EventStream(streamId, version, events);
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
    }

    public async Task<LoadEventsResult> LoadEventsAsync(
        string? partitionKey,
        DateTime? dateFrom = null,
        int limit = 250,
        string? continuationToken = null,
        CancellationToken cancellationToken = default
    ) {
        var connectionInformation = ConnectionInformation;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        List<string> wheres = new List<string>();
        List<NpgsqlParameter> parameters = new List<NpgsqlParameter>();

        if (!string.IsNullOrEmpty(partitionKey))
        {
            wheres.Add("partition_key = @partitionKey");
            parameters.Add(new("partitionKey", partitionKey));
        }

        // Keyset pagination: use (created_at, id) composite cursor from continuation token
        if (!string.IsNullOrEmpty(continuationToken) && TryParseContinuationToken(continuationToken, out var cursorDate, out var cursorId))
        {
            wheres.Add("(created_at, id) > (@cursorDate, @cursorId)");
            parameters.Add(new("cursorDate", cursorDate));
            parameters.Add(new("cursorId", cursorId));
        }
        else if (dateFrom.HasValue)
        {
            wheres.Add("created_at >= @createdAt");
            parameters.Add(new("createdAt", dateFrom));
        }

        await using var cmd = new NpgsqlCommand(
            $"SELECT id, created_at, event_type, event_data " +
            $"FROM \"{connectionInformation.TableName}\" " +
            (wheres.Count > 0
                ? $"WHERE {string.Join(" AND ", wheres)} "
                : "") +
            $"ORDER BY created_at ASC, id ASC " +
            $"LIMIT {limit}", conn
        );

        cmd.Parameters.AddRange(parameters.ToArray());

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var events = new List<IEvent>();
            Guid lastId = Guid.Empty;
            DateTime lastCreatedAt = DateTime.MinValue;

            while (await reader.ReadAsync(cancellationToken))
            {
                lastId = reader.GetGuid("id");
                lastCreatedAt = reader.GetDateTime("created_at");

                var eventWrapper = new EventWrapper()
                {
                    Id = lastId,
                    EventType = reader.GetString("event_type"),
                    EventData = JsonSerializer.Deserialize<JsonElement>(reader.GetString("event_data"))
                };

                events.Add(eventWrapper.GetEvent());
            }

            return new LoadEventsResult
            {
                Events = events,
                ContinuationToken = events.Count > 0
                    ? BuildContinuationToken(lastCreatedAt, lastId)
                    : null
            };
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
    }

    private static string BuildContinuationToken(DateTime createdAt, Guid id)
    {
        return $"{createdAt:O}|{id}";
    }

    private static bool TryParseContinuationToken(string token, out DateTime createdAt, out Guid id)
    {
        createdAt = default;
        id = default;

        var parts = token.Split('|', 2);
        if (parts.Length != 2)
        {
            return false;
        }

        return DateTime.TryParse(parts[0], System.Globalization.CultureInfo.InvariantCulture,
                   System.Globalization.DateTimeStyles.RoundtripKind, out createdAt)
               && Guid.TryParse(parts[1], out id);
    }

    public async Task<bool> AppendToStreamAsync(EventUserInfo eventUserInfo, Guid streamId, int expectedVersion, IEnumerable<IEvent> events, CancellationToken cancellationToken = default)
    {
        if (streamId == Guid.Empty)
        {
            throw new ArgumentNullException($"{nameof(streamId)} cannot be an empty Guid");
        }

        var eventsList = events as IList<IEvent> ?? events.ToList();

        var connectionInformation = ConnectionInformation;

        if (eventsList.GroupBy(x => x.PartitionKey).Count() != 1)
        {
            throw new ArgumentException("Partition keys for all events in the stream must be the same");
        }

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT MAX(stream_version) FROM \"{connectionInformation.TableName}\" WHERE stream_id = @streamId", conn, transaction)
        {
            Parameters =
            {
                new("streamId", streamId)
            }
        };

        try
        {
            var version = await cmd.ExecuteScalarAsync(cancellationToken);

            if (version != null && version is not DBNull && (int)version != expectedVersion)
            {
                return false;
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

        await using var batchInsert = new NpgsqlBatch(conn, transaction);

        foreach (var evt in eventsList)
        {
            batchInsert.BatchCommands.Add(new NpgsqlBatchCommand($"" +
                $"INSERT INTO \"{connectionInformation.TableName}\" " +
                $"(id, partition_key, created_at, stream_id, stream_version, event_type, event_data, user_info, eventstore_schema_version) " +
                $"VALUES " +
                $"(@id, @partition_key, @created_at, @stream_id, @stream_version, @event_type, @event_data, @user_info, @eventstore_schema_version)")
            {
                Parameters =
                {
                    new("id", Guid.NewGuid()),
                    new("partition_key", evt.PartitionKey),
                    new("created_at", evt.Timestamp),
                    new("stream_id", streamId),
                    new("stream_version", ++expectedVersion),
                    new("event_type", evt.GetType().AssemblyQualifiedName),
                    new NpgsqlParameter()
                    {
                        ParameterName = "event_data",
                        Value = JsonSerializer.Serialize(evt, evt.GetType(), EventStoreSerializerOptions.Options),
                        DataTypeName = "jsonb"
                    },
                    new NpgsqlParameter()
                    {
                        ParameterName = "user_info",
                        Value = JsonSerializer.Serialize(eventUserInfo, eventUserInfo.GetType(), EventStoreSerializerOptions.Options),
                        DataTypeName = "jsonb"
                    },
                    new NpgsqlParameter("eventstore_schema_version", EVENTSTORE_TABLE_SCHEMA_VERSION)
                }
            });
        }

        try
        {
            await batchInsert.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            return false;
        }

        var handlers = _eventAddedEventHandlers.ToList();
        foreach (var e in eventsList)
        {
            foreach (var h in handlers)
            {
                await h(e);
            }
        }

        return true;
    }

    public void SubscribeToEventAdded(Func<IEvent, Task> handler)
    {
        _eventAddedEventHandlers.Add(handler);
    }

    public void UnsubscribeFromEventAdded(Func<IEvent, Task> handler)
    {
        _eventAddedEventHandlers.Remove(handler);
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken = default)
    {
        var connectionInformation = ConnectionInformation;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            $"SELECT 1 FROM \"{connectionInformation.TableName}\"", conn)
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
                    "CREATE OR REPLACE FUNCTION to_timestamp_utc(text)" +
                    "RETURNS timestamp with time zone AS" +
                    "    $func$" +
                    "SELECT $1::timestamp without time zone at time zone 'UTC'" +
                    "    $func$ LANGUAGE sql IMMUTABLE;" +
                    "" +
                    $"CREATE TABLE \"{connectionInformation.TableName}\" (" +
                    $"  id uuid, " +
                    $"  partition_key varchar(36), " +
                    $"  created_at timestamp without time zone, " +
                    $"  stream_id uuid, " +
                    $"  stream_version integer, " +
                    $"  event_type varchar(500), " +
                    $"  event_data jsonb, " +
                    $"  user_info jsonb, " +
                    $"  eventstore_schema_version int NOT NULL, " +
                    $"  UNIQUE (stream_id, partition_key, stream_version)" +
                    $");" +
                    $"CREATE INDEX \"{connectionInformation.TableName}_stream_id_idx\" " +
                    $"  ON \"{connectionInformation.TableName}\" (stream_id);" +
                    $"CREATE INDEX \"{connectionInformation.TableName}_stream_id_with_partition_key_idx\" " +
                    $"  ON \"{connectionInformation.TableName}\" (stream_id, partition_key);" +
                    $"CREATE INDEX \"{connectionInformation.TableName}_created_at\" " +
                    $"  ON \"{connectionInformation.TableName}\" (created_at);"
                    
                , conn);

                await createTableCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await using var cmdItemTable = new NpgsqlCommand(
            $"SELECT 1 FROM \"{connectionInformation.MetadataTableName}\"", conn)
        {
        };

        try
        {
            await cmdItemTable.ExecuteScalarAsync(cancellationToken);
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

    public async Task<bool> AppendGlobalEventAsync(
        EventUserInfo eventUserInfo,
        ICrossAggregateEvent @event,
        CancellationToken cancellationToken = default
    )
    {
        var connectionInformation = ConnectionInformation;
        var streamId = DeterministicGuid.Create(@event.AggregateType);
        var partitionKey = @event.TargetPartitionKey ?? CrossAggregateEvent.AllPartitionsKey;

        @event.PartitionKey = partitionKey;

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        // Get current max version for this global stream
        await using var versionCmd = new NpgsqlCommand(
            $"SELECT COALESCE(MAX(stream_version), 0) FROM \"{connectionInformation.TableName}\" WHERE stream_id = @streamId AND partition_key = @partitionKey",
            conn, transaction)
        {
            Parameters =
            {
                new("streamId", streamId),
                new("partitionKey", partitionKey)
            }
        };

        int nextVersion;
        try
        {
            var currentVersion = await versionCmd.ExecuteScalarAsync(cancellationToken);
            nextVersion = (currentVersion is int v ? v : 0) + 1;
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

        await using var insertCmd = new NpgsqlCommand(
            $"INSERT INTO \"{connectionInformation.TableName}\" " +
            $"(id, partition_key, created_at, stream_id, stream_version, event_type, event_data, user_info, eventstore_schema_version) " +
            $"VALUES (@id, @partition_key, @created_at, @stream_id, @stream_version, @event_type, @event_data, @user_info, @eventstore_schema_version)",
            conn, transaction)
        {
            Parameters =
            {
                new("id", Guid.NewGuid()),
                new("partition_key", partitionKey),
                new("created_at", @event.Timestamp),
                new("stream_id", streamId),
                new("stream_version", nextVersion),
                new("event_type", @event.GetType().AssemblyQualifiedName),
                new NpgsqlParameter()
                {
                    ParameterName = "event_data",
                    Value = JsonSerializer.Serialize(@event, @event.GetType(), EventStoreSerializerOptions.Options),
                    DataTypeName = "jsonb"
                },
                new NpgsqlParameter()
                {
                    ParameterName = "user_info",
                    Value = JsonSerializer.Serialize(eventUserInfo, eventUserInfo.GetType(), EventStoreSerializerOptions.Options),
                    DataTypeName = "jsonb"
                },
                new NpgsqlParameter("eventstore_schema_version", EVENTSTORE_TABLE_SCHEMA_VERSION)
            }
        };

        try
        {
            await insertCmd.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            return false;
        }

        // Notify handlers
        var handlers = _eventAddedEventHandlers.ToList();
        foreach (var h in handlers)
        {
            await h(@event);
        }

        return true;
    }

    public async Task<List<IEvent>> LoadGlobalEventsAsync(
        string aggregateType,
        string? partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        var connectionInformation = ConnectionInformation;
        var streamId = DeterministicGuid.Create(aggregateType);

        await using var conn = new NpgsqlConnection(connectionInformation.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        string sql;
        NpgsqlCommand cmd;

        if (partitionKey != null)
        {
            sql = $"SELECT id, stream_id, stream_version, event_type, event_data, user_info " +
                  $"FROM \"{connectionInformation.TableName}\" " +
                  $"WHERE stream_id = @streamId AND (partition_key = @partitionKey OR partition_key = @allPartitionsKey) " +
                  $"ORDER BY created_at ASC";
            cmd = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new("streamId", streamId),
                    new("partitionKey", partitionKey),
                    new("allPartitionsKey", CrossAggregateEvent.AllPartitionsKey)
                }
            };
        }
        else
        {
            sql = $"SELECT id, stream_id, stream_version, event_type, event_data, user_info " +
                  $"FROM \"{connectionInformation.TableName}\" " +
                  $"WHERE stream_id = @streamId " +
                  $"ORDER BY created_at ASC";
            cmd = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new("streamId", streamId)
                }
            };
        }

        await using (cmd)
        {
            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                var events = new List<IEvent>();

                while (await reader.ReadAsync(cancellationToken))
                {
                    var eventWrapper = new EventWrapper()
                    {
                        Id = reader.GetGuid("id"),
                        StreamInfo = new StreamInfo
                        {
                            Id = reader.GetGuid("stream_id"),
                            Version = reader.GetInt16("stream_version")
                        },
                        EventType = reader.GetString("event_type"),
                        EventData = JsonSerializer.Deserialize<JsonElement>(reader.GetString("event_data")),
                        UserInfo = JsonSerializer.Deserialize<JsonElement>(reader.GetString("user_info")),
                    };

                    events.Add(eventWrapper.GetEvent());
                }

                return events;
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
        }
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}