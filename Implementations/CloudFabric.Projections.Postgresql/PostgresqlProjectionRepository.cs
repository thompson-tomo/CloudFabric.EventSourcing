using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CloudFabric.Projections.Exceptions;
using CloudFabric.Projections.Queries;
using CloudFabric.Projections.Resilience;
using CloudFabric.Projections.Utils;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Polly;

namespace CloudFabric.Projections.Postgresql;

public class QueryChunk
{
    public string WhereChunk { get; set; } = "";
    public List<NpgsqlParameter> Parameters { get; init; } = new ();
    public List<string> AdditionalFromSelects { get; init; } = new();
}

public class PostgresqlProjectionRepository<TProjectionDocument> : PostgresqlProjectionRepository, IProjectionRepository<TProjectionDocument>
    where TProjectionDocument : ProjectionDocument
{
    public PostgresqlProjectionRepository(
        string connectionString,
        ILoggerFactory loggerFactory,
        bool includeDebugInformation = false,
        ResilienceSettings? resilienceSettings = null)
        : base(connectionString, ProjectionDocumentSchemaFactory.FromTypeWithAttributes<TProjectionDocument>(), loggerFactory, includeDebugInformation, resilienceSettings)
    {
    }

    public new async Task<TProjectionDocument?> Single(
        Guid id,
        string partitionKey, 
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    ) {        
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }
        
        var document = await base.Single(id, partitionKey, cancellationToken, indexSelector);

        if (document == null) return null;

        return ProjectionDocumentSerializer.DeserializeFromDictionary<TProjectionDocument>(document);
    }

    public Task Upsert(
        TProjectionDocument document, 
        string partitionKey, 
        DateTime updatedAt, 
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    ) {
        if (document == null)
        {
            throw new ArgumentNullException(nameof(document));
        }
        
        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }
        
        var documentDictionary = ProjectionDocumentSerializer.SerializeToDictionary(document);
        return Upsert(documentDictionary, partitionKey, updatedAt, cancellationToken, indexSelector);
    }

    public new async Task<ProjectionQueryResult<TProjectionDocument>> Query(
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    )
    {
        if (projectionQuery == null)
        {
            throw new ArgumentNullException(nameof(projectionQuery));
        }
        
        var recordsDictionary = await base.Query(
            projectionQuery, partitionKey, cancellationToken, indexSelector
        );

        var records = new List<QueryResultDocument<TProjectionDocument>>();

        foreach (var doc in recordsDictionary.Records)
        {
            records.Add(
                new QueryResultDocument<TProjectionDocument>
                {
                    Document = ProjectionDocumentSerializer.DeserializeFromDictionary<TProjectionDocument>(doc.Document)
                }
            );
        }

        return new ProjectionQueryResult<TProjectionDocument>
        {
            DebugInformation = recordsDictionary.DebugInformation,
            IndexName = recordsDictionary.IndexName,
            TotalRecordsFound = recordsDictionary.TotalRecordsFound,
            Records = records
        };
    }
}

public class PostgresqlProjectionRepository : ProjectionRepository
{
    private bool _includeDebugInformation = false;

    private readonly string _connectionString;

    private string? _keyPropertyName;
    private string? _tableName;

    private readonly ResiliencePipeline _retryPipeline;

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08001", "08003", "08004", "08006", "40001", "40P01", "57P03", "53300"
    };

    private static bool IsTransientSqlState(string? sqlState) =>
        sqlState != null && TransientSqlStates.Contains(sqlState);

    public PostgresqlProjectionRepository(
        string connectionString,
        ProjectionDocumentSchema projectionDocumentSchema,
        ILoggerFactory loggerFactory,
        bool includeDebugInformation = false,
        ResilienceSettings? resilienceSettings = null
    ) : base(projectionDocumentSchema, loggerFactory.CreateLogger<ProjectionRepository>())
    {
        _connectionString = connectionString;
        _includeDebugInformation = includeDebugInformation;
        _retryPipeline = BuildRetryPipeline(resilienceSettings ?? ResilienceSettings.Default, loggerFactory);

        // for dynamic projection document schemas we need to ensure 'partitionKey' column is always there
        if (ProjectionDocumentSchema.Properties.All(p => p.PropertyName != "PartitionKey"))
        {
            ProjectionDocumentSchema.Properties.Add(new ProjectionDocumentPropertySchema()
            {
                PropertyName = "PartitionKey",
                PropertyType = TypeCode.String,
                IsFilterable = true
            });
        }
    }

    private ResiliencePipeline BuildRetryPipeline(ResilienceSettings settings, ILoggerFactory loggerFactory)
    {
        var shouldHandle = new PredicateBuilder<object>()
            .Handle<NpgsqlException>(ex => ex.IsTransient || IsTransientSqlState(ex.SqlState))
            .Handle<TimeoutException>();

        return ResiliencePipelineFactory.Create(
            settings,
            shouldHandle,
            loggerFactory.CreateLogger<PostgresqlProjectionRepository>(),
            "PostgresqlProjectionRepository"
        );
    }

    public string TableName
    {
        get
        {
            if (string.IsNullOrEmpty(_tableName))
            {
                _tableName = ProjectionDocumentSchema.SchemaName;
            }

            return _tableName;
        }
    }

    public string? KeyColumnName
    {
        get
        {
            if (string.IsNullOrEmpty(_keyPropertyName))
            {
                _keyPropertyName = ProjectionDocumentSchema.KeyColumnName;
            }

            return _keyPropertyName;
        }
    }
    
    protected override async Task CreateIndex(string indexName, ProjectionDocumentSchema projectionDocumentSchema)
    {
        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            var createTableSql = ConstructCreateTableSql(indexName, projectionDocumentSchema);

            await using var createTableCommand = new NpgsqlCommand(createTableSql, conn);
            try
            {
                await createTableCommand.ExecuteNonQueryAsync(ct);
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState != PostgresErrorCodes.DuplicateTable) // table already created, can be ignored
                {
                    throw;
                }
            }
            catch (Exception createTableException)
            {
                var exception = new Exception($"Failed to create a table for projection \"{TableName}\"", createTableException);
                exception.Data.Add("commandText", createTableSql);
                throw exception;
            }

            var createIndexesSql = ConstructCreateIndexesSql(indexName, projectionDocumentSchema);
            if (!string.IsNullOrEmpty(createIndexesSql))
            {
                await using var createIndexesCommand = new NpgsqlCommand(createIndexesSql, conn);
                await createIndexesCommand.ExecuteNonQueryAsync(ct);
            }
        }, CancellationToken.None);
    }

    protected override async Task DropIndex(string indexName, CancellationToken cancellationToken = default)
    {
        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS \"{indexName}\"", conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }, cancellationToken);
    }

    protected override async Task<Dictionary<string, object?>?> SingleInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    ) {
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }

        if (indexDescriptor.ProjectionDocumentSchema.Properties.Count <= 0)
        {
            throw new ArgumentException(
                "Projection document schema has no properties",
                indexDescriptor.ProjectionDocumentSchema.SchemaName
            );
        }

        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"SELECT " +
                string.Join(',', indexDescriptor.ProjectionDocumentSchema.Properties.Select(p => p.PropertyName)) + " " +
                $"FROM \"{indexDescriptor.IndexName}\" " +
                $"WHERE {KeyColumnName} = @id AND {nameof(ProjectionDocument.PartitionKey)} = @partitionKey " +
                $"LIMIT 1", conn
            )
            {
                Parameters =
                {
                    new(KeyColumnName, id),
                    new("partitionKey", partitionKey)
                }
            };

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (reader.HasRows)
                {
                    var result = new Dictionary<string, object?>();

                    while (await reader.ReadAsync(ct))
                    {
                        var values = new object[indexDescriptor.ProjectionDocumentSchema.Properties.Count];
                        reader.GetValues(values);

                        if (values.Length <= 0)
                        {
                            return null;
                        }

                        for (var i = 0; i < indexDescriptor.ProjectionDocumentSchema.Properties.Count; i++)
                        {
                            if (values[i] is DBNull)
                            {
                                result[indexDescriptor.ProjectionDocumentSchema.Properties[i].PropertyName] = null;
                            }
                            // try to check whether the property is a json object or array
                            else if (
                                (indexDescriptor.ProjectionDocumentSchema.Properties[i].IsNestedObject || indexDescriptor.ProjectionDocumentSchema.Properties[i].IsNestedArray)
                                && values[i] is string
                            )
                            {
                                result[indexDescriptor.ProjectionDocumentSchema.Properties[i].PropertyName] =
                                    JsonToObjectConverter.Convert((string)values[i], indexDescriptor.ProjectionDocumentSchema.Properties[i]);
                            }
                            else
                            {
                                result[indexDescriptor.ProjectionDocumentSchema.Properties[i].PropertyName] = values[i];
                            }
                        }
                    }

                    return result;
                }

                return null;
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                {
                    throw new InvalidProjectionSchemaException(ex);
                }
                else
                {
                    throw new Exception(
                        $"Something went terribly wrong while updating/inserting document in \"{TableName}\".",
                        ex
                    );
                }
            }

            return null;
        }, cancellationToken);
    }

    protected override async Task DeleteInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    ) {
        if (id == Guid.Empty)
        {
            throw new ArgumentNullException(nameof(id));
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }

        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"DELETE " +
                $" FROM \"{indexDescriptor.IndexName}\" WHERE {KeyColumnName} = @id AND {nameof(ProjectionDocument.PartitionKey)} = @partitionKey", conn
            )
            {
                Parameters =
                {
                    new("id", id),
                    new("partitionKey", partitionKey)
                }
            };

            await cmd.ExecuteNonQueryAsync(ct);
        }, cancellationToken);
    }

    public override async Task DeleteAll(
        string? partitionKey = null, 
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    ) {
        var indexState = await GetProjectionIndexState(cancellationToken);

        if (indexState == null)
        {
            return;
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        
        foreach (var indexStatus in indexState.IndexesStatuses)
        {
            if (partitionKey == null)
            {
                await using var dropTableCmd = new NpgsqlCommand(
                    $"DROP TABLE IF EXISTS \"{indexStatus.IndexName}\" ", conn
                );
                await dropTableCmd.ExecuteNonQueryAsync(cancellationToken);
            }
            else
            {
                await using var cmd = new NpgsqlCommand(
                    $"DELETE " +
                    $" FROM \"{indexStatus.IndexName}\" " +
                    (!string.IsNullOrEmpty(partitionKey) ? $" WHERE {nameof(ProjectionDocument.PartitionKey)} = @partitionKey" : ""), conn
                );

                if (!string.IsNullOrEmpty(partitionKey))
                {
                    cmd.Parameters.Add(new("partitionKey", partitionKey));
                }

                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        indexState.IndexesStatuses.Clear();
        await SaveProjectionIndexState(indexState);
        //
        // await using var dropIndexTableCmd = new NpgsqlCommand(
        //     $"DROP TABLE \"{PROJECTION_INDEX_STATE_INDEX_NAME}\" ", conn
        // );
        // await dropIndexTableCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    protected override async Task UpsertInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    ) {
        if (document == null)
        {
            throw new ArgumentNullException(nameof(document));
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }

        if (indexDescriptor.ProjectionDocumentSchema.Properties.Count <= 0)
        {
            throw new ArgumentException(
                "Projection document schema has no properties",
                indexDescriptor.ProjectionDocumentSchema.SchemaName
            );
        }

        document[nameof(ProjectionDocument.PartitionKey)] = partitionKey;
        document[nameof(ProjectionDocument.UpdatedAt)] = updatedAt;

        var propertiesToInsert = indexDescriptor.ProjectionDocumentSchema.Properties
            .Where(p => document.Keys.Contains(p.PropertyName)).ToList(); // document may not contain non-required properties,
                                                                          // we need to exclude them from query

        var propertyNames = propertiesToInsert
            .Select(p => p.PropertyName)
            .ToArray();

        // Serialize nested objects/arrays before entering the retry loop
        foreach (var p in propertiesToInsert)
        {
            if (p.IsNestedObject || p.IsNestedArray)
            {
                document[p.PropertyName] = JsonSerializer.SerializeToDocument(document[p.PropertyName]);
            }
        }

        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO \"{indexDescriptor.IndexName}\" ({string.Join(',', propertyNames)}) " +
                $"VALUES ({string.Join(',', propertyNames.Select(p => $"@{p}"))}) " +
                $"ON CONFLICT ({indexDescriptor.ProjectionDocumentSchema.KeyColumnName}) " +
                $"DO UPDATE SET {string.Join(',', propertyNames.Select(p => $"{p} = @{p}"))} "
                , conn
            );

            foreach (var p in propertiesToInsert)
            {
                cmd.Parameters.Add(new(p.PropertyName, document[p.PropertyName] ?? DBNull.Value));
            }

            try
            {
                var updatedRows = await cmd.ExecuteNonQueryAsync(ct);

                if (updatedRows != 1)
                {
                    throw new Exception("Something happened with upsert operation: no rows were affected");
                }
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                {
                    throw new InvalidProjectionSchemaException(ex);
                }
                else
                {
                    throw new Exception(
                        $"Something went terribly wrong while updating/inserting document in \"{TableName}\".",
                        ex
                    );
                }
            }
        }, cancellationToken);
    }
    
    /// <summary>
    /// Bulk upsert using multi-row INSERT ... ON CONFLICT DO UPDATE SET ... = EXCLUDED.
    /// Sub-batches by PostgreSQL parameter limit (~65535 params).
    /// </summary>
    protected override async Task FlushBufferAsync(
        ProjectionOperationIndexDescriptor indexDescriptor,
        IReadOnlyList<BufferedUpsert> items,
        CancellationToken cancellationToken = default)
    {
        if (items.Count == 0) return;

        await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            var propertiesToInsert = indexDescriptor.ProjectionDocumentSchema.Properties;
            var propertyNames = propertiesToInsert.Select(p => p.PropertyName).ToArray();
            var keyColumnName = indexDescriptor.ProjectionDocumentSchema.KeyColumnName;

            // PostgreSQL max parameters ~65535; sub-batch to stay within limits
            var maxParamsPerRow = propertyNames.Length;
            var maxRowsPerBatch = maxParamsPerRow > 0 ? Math.Max(1, 65000 / maxParamsPerRow) : items.Count;

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            for (var offset = 0; offset < items.Count; offset += maxRowsPerBatch)
            {
                var batchSlice = items.Skip(offset).Take(maxRowsPerBatch).ToList();

                var sb = new StringBuilder();
                sb.Append($"INSERT INTO \"{indexDescriptor.IndexName}\" ({string.Join(',', propertyNames)}) VALUES ");

                var allParams = new List<NpgsqlParameter>();

                for (var rowIdx = 0; rowIdx < batchSlice.Count; rowIdx++)
                {
                    var item = batchSlice[rowIdx];
                    var doc = item.Document;
                    doc[nameof(ProjectionDocument.PartitionKey)] = item.PartitionKey;
                    doc[nameof(ProjectionDocument.UpdatedAt)] = item.UpdatedAt;

                    if (rowIdx > 0) sb.Append(',');
                    sb.Append('(');

                    for (var colIdx = 0; colIdx < propertiesToInsert.Count; colIdx++)
                    {
                        var prop = propertiesToInsert[colIdx];
                        var paramName = $"{prop.PropertyName}_{rowIdx}";

                        if (colIdx > 0) sb.Append(',');
                        sb.Append($"@{paramName}");

                        var value = doc.TryGetValue(prop.PropertyName, out var v) ? v : null;

                        if (prop.IsNestedObject || prop.IsNestedArray)
                        {
                            value = JsonSerializer.SerializeToDocument(value);
                        }

                        allParams.Add(new NpgsqlParameter(paramName, value ?? DBNull.Value));
                    }

                    sb.Append(')');
                }

                sb.Append($" ON CONFLICT ({keyColumnName}) DO UPDATE SET ");
                sb.Append(string.Join(',', propertyNames.Select(p => $"{p} = EXCLUDED.{p}")));

                await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
                cmd.Parameters.AddRange(allParams.ToArray());

                try
                {
                    await cmd.ExecuteNonQueryAsync(ct);
                }
                catch (NpgsqlException ex)
                {
                    if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                    {
                        throw new InvalidProjectionSchemaException(ex);
                    }

                    throw new Exception(
                        $"FlushBufferAsync failed on \"{indexDescriptor.IndexName}\" (batch of {batchSlice.Count} rows).",
                        ex
                    );
                }
            }
        }, cancellationToken);
    }

    protected override async Task<ProjectionQueryResult<Dictionary<string, object?>>> QueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default
    )
    {
        if (projectionQuery == null)
        {
            throw new ArgumentNullException(nameof(projectionQuery));
        }

        indexDescriptor.ProjectionDocumentSchema ??= ProjectionDocumentSchema;

        var properties = indexDescriptor.ProjectionDocumentSchema.Properties;

        var queryChunk = ConstructConditionFilters(projectionQuery.Filters, indexDescriptor.ProjectionDocumentSchema);

        var fromStatements = new List<string>()
        {
            "\"" + indexDescriptor.IndexName + "\""
        };

        fromStatements.AddRange(queryChunk.AdditionalFromSelects);

        var sb = new StringBuilder();
        sb.Append("SELECT ");
        sb.AppendJoin(',', properties.Select(p => p.PropertyName));
        sb.Append(" FROM ");
        sb.Append(string.Join(", ", fromStatements));

        if (!string.IsNullOrEmpty(partitionKey))
        {
            queryChunk.WhereChunk += string.IsNullOrWhiteSpace(queryChunk.WhereChunk)
                ? $" {nameof(ProjectionDocument.PartitionKey)} = @partitionKey"
                : $" AND {nameof(ProjectionDocument.PartitionKey)} = @partitionKey";
            queryChunk.Parameters.Add(new("partitionKey", partitionKey));
        }

        if (!string.IsNullOrWhiteSpace(projectionQuery.SearchText) && projectionQuery.SearchText != "*")
        {
            var (searchQuery, searchParams) = ConstructSearchQuery(projectionQuery.SearchText, indexDescriptor.ProjectionDocumentSchema);
            queryChunk.WhereChunk += string.IsNullOrWhiteSpace(queryChunk.WhereChunk) ? $" {searchQuery}" : $" AND {searchQuery}";
            queryChunk.Parameters.AddRange(searchParams);
        }

        if (!string.IsNullOrEmpty(queryChunk.WhereChunk))
        {
            sb.Append(" WHERE ");
            sb.Append(queryChunk.WhereChunk);
        }

        sb.Append(" GROUP BY id");

        // total count query -- use COUNT(DISTINCT id) to avoid inflated counts from jsonb_array_elements joins
        string totalCountQuery = $"SELECT COUNT(DISTINCT id) FROM {string.Join(", ", fromStatements)}";
        if (!string.IsNullOrEmpty(queryChunk.WhereChunk))
        {
            totalCountQuery += $" WHERE {queryChunk.WhereChunk}";
        }

        NpgsqlParameter[] totalCountParams = new NpgsqlParameter[queryChunk.Parameters.Count];
        queryChunk.Parameters.CopyTo(totalCountParams);

        if (projectionQuery.OrderBy.Count > 0)
        {
            var orderByClauses = new List<string>();
            for (var sortIdx = 0; sortIdx < projectionQuery.OrderBy.Count; sortIdx++)
            {
                var kv = projectionQuery.OrderBy[sortIdx];

                if (!Regex.IsMatch(kv.KeyPath, @"^[a-zA-Z_][a-zA-Z0-9_.]*$"))
                {
                    throw new ProjectionQueryFilterException($"Invalid sort key: {kv.KeyPath}");
                }

                if (!string.Equals(kv.Order, "asc", StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(kv.Order, "desc", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ProjectionQueryFilterException($"Invalid sort order: {kv.Order}");
                }

                var pathParts = kv.KeyPath.Split('.');
                if (pathParts.Length > 1)
                {
                    var topLevelProp = indexDescriptor.ProjectionDocumentSchema.Properties
                        .FirstOrDefault(p => p.PropertyName == pathParts[0]);

                    if (topLevelProp is { IsNestedObject: true })
                    {
                        var jsonPath = $"{pathParts[0]}->>{string.Join("->>", pathParts.Skip(1).Select(p => $"'{p}'"))}";
                        orderByClauses.Add($"{jsonPath} {kv.Order} NULLS LAST");
                        continue;
                    }

                    if (topLevelProp is { IsNestedArray: true })
                    {
                        if (kv.Filters.Any())
                        {
                            var filterConditions = new List<string>();
                            for (var fi = 0; fi < kv.Filters.Count; fi++)
                            {
                                var filter = kv.Filters[fi];
                                var filterParts = filter.FilterKeyPath.Split('.');
                                var filterPropName = filterParts.Length > 1 ? filterParts[^1] : filterParts[0];

                                var sortFilterParamName = $"sortFilter_{sortIdx}_{fi}";
                                queryChunk.Parameters.Add(new NpgsqlParameter(sortFilterParamName, filter.FilterValue));

                                var elemAccess = $"_sort_elem->>'{filterPropName}'";
                                if (filter.FilterValue is decimal)
                                    elemAccess = $"({elemAccess})::decimal";
                                else if (filter.FilterValue is int)
                                    elemAccess = $"({elemAccess})::int";
                                else if (filter.FilterValue is long)
                                    elemAccess = $"({elemAccess})::bigint";
                                else if (filter.FilterValue is Guid)
                                    elemAccess = $"({elemAccess})::uuid";

                                filterConditions.Add($"{elemAccess} = @{sortFilterParamName}");
                            }

                            var sortPropName = pathParts[^1];
                            var subquery = $"(SELECT _sort_elem->>'{sortPropName}' FROM jsonb_array_elements({pathParts[0]}) _sort_elem " +
                                           $"WHERE {string.Join(" AND ", filterConditions)} LIMIT 1)";
                            orderByClauses.Add($"{subquery} {kv.Order} NULLS LAST");
                        }
                        else
                        {
                            var sortPropName = pathParts[^1];
                            orderByClauses.Add($"({pathParts[0]}->0->>'{sortPropName}') {kv.Order} NULLS LAST");
                        }

                        continue;
                    }
                }

                orderByClauses.Add($"{kv.KeyPath} {kv.Order}");
            }

            sb.Append(" ORDER BY ");
            sb.Append(string.Join(',', orderByClauses));
        }

        if (projectionQuery.Limit.HasValue)
        {
            sb.Append(" LIMIT @limit");
            queryChunk.Parameters.Add(new NpgsqlParameter("limit", projectionQuery.Limit.Value));
        }

        sb.Append(" OFFSET @offset");
        queryChunk.Parameters.Add(new NpgsqlParameter("offset", projectionQuery.Offset));

        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            try
            {
                // calculate total count
                await using var totalCountCmd = new NpgsqlCommand(totalCountQuery, conn);
                totalCountCmd.Parameters.AddRange(totalCountParams.Select(p => p.Clone()).ToArray());

                var totalCount = await totalCountCmd.ExecuteScalarAsync(ct) as long?;
                totalCountCmd.Parameters.Clear();

                var commandText = sb.ToString();

                Logger.LogTrace("Executing command: {CommandText} with parameters: {Parameters}",
                    commandText,
                    string.Join(", ", queryChunk.Parameters.Select(p => $"{p.ParameterName} = {p.Value}"))
                );

                await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
                cmd.Parameters.AddRange(queryChunk.Parameters.Select(p => p.Clone()).ToArray());

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var records = new List<Dictionary<string, object?>>();
                while (await reader.ReadAsync(ct))
                {
                    var document = new Dictionary<string, object?>();

                    var values = new object[properties.Count];
                    reader.GetValues(values);

                    if (values.Length <= 0)
                    {
                        continue;
                    }

                    for (var i = 0; i < properties.Count; i++)
                    {
                        if (values[i] is DBNull)
                        {
                            document[indexDescriptor.ProjectionDocumentSchema.Properties[i].PropertyName] = null;
                        }
                        // try to check whether the property is a json object or array
                        else if (
                            (indexDescriptor.ProjectionDocumentSchema.Properties[i].IsNestedObject || indexDescriptor.ProjectionDocumentSchema.Properties[i].IsNestedArray)
                            && values[i] is string
                        )
                        {
                            document[properties[i].PropertyName] = JsonToObjectConverter.Convert((string)values[i], properties[i]);
                        }
                        else
                        {
                            document[properties[i].PropertyName] = values[i];
                        }
                    }

                    records.Add(document);
                }

                var debugInformation = "";

                if (_includeDebugInformation)
                {
                    debugInformation += cmd.CommandText;

                    foreach (NpgsqlParameter param in cmd.Parameters)
                    {
                        var paramValue = "";

                        switch (param.NpgsqlDbType)
                        {
                            case NpgsqlDbType.Uuid:
                            case NpgsqlDbType.Text:
                                paramValue = $"'{param.NpgsqlValue}'";
                                break;
                            default:
                                paramValue = param.NpgsqlValue?.ToString();
                                break;
                        }

                        debugInformation = debugInformation.Replace($"@{param.ParameterName}", paramValue);
                    }

                    debugInformation += $"\n\nOriginal command:\n{cmd.CommandText}; " +
                        $"{string.Join(',', cmd.Parameters.Select(p => $"@{p.ParameterName}:{p.NpgsqlDbType}={p.NpgsqlValue}"))}";
                }

                return new ProjectionQueryResult<Dictionary<string, object?>>
                {
                    DebugInformation = _includeDebugInformation ? debugInformation : String.Empty,
                    IndexName = TableName,
                    TotalRecordsFound = totalCount,
                    Records = records.Select(x =>
                        new QueryResultDocument<Dictionary<string, object?>>
                        {
                            Document = x
                        }
                    ).ToList()
                };
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                {
                    throw new InvalidProjectionSchemaException(ex);
                }
                else
                {
                    throw new Exception(
                        $"Something went terribly wrong while querying \"{TableName}\".",
                        ex
                    );
                }
            }
        }, cancellationToken);
    }
    
    protected override async Task<long> UpdateByQueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery query,
        string? partitionKey,
        Dictionary<string, object?> propertyUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        indexDescriptor.ProjectionDocumentSchema ??= ProjectionDocumentSchema;

        var queryChunk = ConstructConditionFilters(query.Filters, indexDescriptor.ProjectionDocumentSchema);

        if (!string.IsNullOrEmpty(partitionKey))
        {
            queryChunk.WhereChunk += string.IsNullOrWhiteSpace(queryChunk.WhereChunk)
                ? $" {nameof(ProjectionDocument.PartitionKey)} = @partitionKey"
                : $" AND {nameof(ProjectionDocument.PartitionKey)} = @partitionKey";
            queryChunk.Parameters.Add(new("partitionKey", partitionKey));
        }

        // Build SET clause from propertyUpdates
        var setStatements = new List<string>();
        var setParameters = new List<NpgsqlParameter>();

        foreach (var (propName, propValue) in propertyUpdates)
        {
            var paramName = $"set_{propName}";
            setStatements.Add($"{propName} = @{paramName}");

            var value = propValue;

            // Handle JSONB serialization for nested objects/arrays
            var propSchema = indexDescriptor.ProjectionDocumentSchema.Properties
                .FirstOrDefault(p => p.PropertyName == propName);
            if (propSchema is { IsNestedObject: true } or { IsNestedArray: true })
            {
                value = JsonSerializer.SerializeToDocument(value);
            }

            setParameters.Add(new(paramName, value ?? DBNull.Value));
        }

        // Always update UpdatedAt
        setStatements.Add($"{nameof(ProjectionDocument.UpdatedAt)} = @set_UpdatedAt");
        setParameters.Add(new("set_UpdatedAt", updatedAt) { NpgsqlDbType = NpgsqlDbType.TimestampTz });

        var sb = new StringBuilder();
        sb.Append($"UPDATE \"{indexDescriptor.IndexName}\" SET ");
        sb.Append(string.Join(", ", setStatements));

        if (!string.IsNullOrEmpty(queryChunk.WhereChunk))
        {
            sb.Append(" WHERE ");
            sb.Append(queryChunk.WhereChunk);
        }

        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            try
            {
                await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
                cmd.Parameters.AddRange(queryChunk.Parameters.Select(p => p.Clone()).ToArray());
                cmd.Parameters.AddRange(setParameters.Select(p => p.Clone()).ToArray());

                return (long)await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                {
                    throw new InvalidProjectionSchemaException(ex);
                }

                throw new Exception(
                    $"Something went terribly wrong while executing UpdateByQuery on \"{indexDescriptor.IndexName}\".",
                    ex
                );
            }
        }, cancellationToken);
    }

    protected override async Task<long> UpdateNestedArrayByQueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery documentQuery,
        string? partitionKey,
        List<NestedArrayUpdate> nestedArrayUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        indexDescriptor.ProjectionDocumentSchema ??= ProjectionDocumentSchema;

        var parameters = new List<NpgsqlParameter>();
        var paramIdx = 0;

        // === SET clauses: one per array update ===
        var setClauses = new List<string>();

        foreach (var arrayUpdate in nestedArrayUpdates)
        {
            var elemAlias = $"_elem{paramIdx}";

            // Element match conditions
            var matchParts = new List<string>();
            foreach (var filter in arrayUpdate.ElementMatchFilters)
            {
                matchParts.Add(BuildElemFilterSql(elemAlias, filter, parameters, ref paramIdx));
            }
            var matchExpr = matchParts.Count > 0 ? string.Join(" AND ", matchParts) : "TRUE";

            // Build update expression by chaining jsonb_set calls
            var updateExpr = elemAlias;
            foreach (var propUpdate in arrayUpdate.ElementUpdates)
            {
                var innerExpr = BuildPropUpdateSql(updateExpr, elemAlias, propUpdate, parameters, ref paramIdx);

                if (propUpdate.Condition != null)
                {
                    var condExpr = BuildElemFilterSql(elemAlias, propUpdate.Condition, parameters, ref paramIdx);
                    updateExpr = $"CASE WHEN {condExpr} THEN {innerExpr} ELSE {updateExpr} END";
                }
                else
                {
                    updateExpr = innerExpr;
                }
            }

            setClauses.Add(
                $"\"{arrayUpdate.ArrayPropertyName}\" = (" +
                $"SELECT jsonb_agg(CASE WHEN {matchExpr} THEN {updateExpr} ELSE {elemAlias} END) " +
                $"FROM jsonb_array_elements(t.\"{arrayUpdate.ArrayPropertyName}\") {elemAlias})"
            );
        }

        setClauses.Add($"\"{nameof(ProjectionDocument.UpdatedAt)}\" = @na_upd");
        parameters.Add(new NpgsqlParameter("na_upd", updatedAt) { NpgsqlDbType = NpgsqlDbType.TimestampTz });

        // === WHERE clause ===
        var whereParts = new List<string>();

        if (!string.IsNullOrEmpty(partitionKey))
        {
            whereParts.Add($"\"{nameof(ProjectionDocument.PartitionKey)}\" = @na_pk");
            parameters.Add(new NpgsqlParameter("na_pk", partitionKey));
        }

        // Group document-level filters: nested array filters → EXISTS subquery, direct → simple condition
        var existsGroups = new Dictionary<string, List<(string nestedProp, Filter filter)>>();
        var directFilters = new List<Filter>();

        foreach (var filter in documentQuery.Filters)
        {
            if (filter.PropertyName != null && filter.PropertyName.Contains('.'))
            {
                var dotIdx = filter.PropertyName.IndexOf('.');
                var arrayProp = filter.PropertyName[..dotIdx];
                var nestedProp = filter.PropertyName[(dotIdx + 1)..];

                if (!existsGroups.ContainsKey(arrayProp))
                    existsGroups[arrayProp] = new List<(string, Filter)>();

                existsGroups[arrayProp].Add((nestedProp, filter));
            }
            else
            {
                directFilters.Add(filter);
            }
        }

        foreach (var (arrayProp, filters) in existsGroups)
        {
            var chkAlias = $"_chk{paramIdx}";
            var existsParts = new List<string>();

            foreach (var (nestedProp, filter) in filters)
            {
                var nestedFilter = new Filter(nestedProp, filter.Operator!, filter.Value);
                existsParts.Add(BuildElemFilterSql(chkAlias, nestedFilter, parameters, ref paramIdx));
            }

            whereParts.Add(
                $"EXISTS (SELECT 1 FROM jsonb_array_elements(t.\"{arrayProp}\") {chkAlias} " +
                $"WHERE {string.Join(" AND ", existsParts)})"
            );
        }

        foreach (var filter in directFilters)
        {
            var pName = $"df{paramIdx}";
            parameters.Add(new NpgsqlParameter(pName, filter.Value ?? DBNull.Value));
            paramIdx++;

            var op = filter.Operator switch
            {
                FilterOperator.Equal => filter.Value == null ? "IS NULL" : $"= @{pName}",
                FilterOperator.StartsWith => $"LIKE @{pName} || '%'",
                _ => $"= @{pName}"
            };
            whereParts.Add($"\"{filter.PropertyName}\" {op}");
        }

        // === Assemble SQL ===
        var sb = new StringBuilder();
        sb.Append($"UPDATE \"{indexDescriptor.IndexName}\" t SET ");
        sb.Append(string.Join(", ", setClauses));

        if (whereParts.Count > 0)
        {
            sb.Append(" WHERE ");
            sb.Append(string.Join(" AND ", whereParts));
        }

        return await _retryPipeline.ExecuteAsync(async (ct) =>
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            try
            {
                await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
                cmd.Parameters.AddRange(parameters.Select(p => p.Clone()).ToArray());
                return (long)await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (NpgsqlException ex)
            {
                if (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedColumn)
                {
                    throw new InvalidProjectionSchemaException(ex);
                }

                throw new Exception(
                    $"Error executing UpdateNestedArrayByQuery on \"{indexDescriptor.IndexName}\": {sb}",
                    ex
                );
            }
        }, cancellationToken);
    }

    private static string BuildElemFilterSql(
        string elemAlias, Filter filter,
        List<NpgsqlParameter> parameters, ref int paramIdx)
    {
        var propAccess = $"{elemAlias}->>'{filter.PropertyName}'";
        var pName = $"ef{paramIdx}";
        paramIdx++;

        switch (filter.Operator)
        {
            case FilterOperator.Equal:
                if (filter.Value == null)
                {
                    return $"({propAccess}) IS NULL";
                }
                parameters.Add(new NpgsqlParameter(pName, filter.Value.ToString()!));
                return $"({propAccess}) = @{pName}";

            case FilterOperator.StartsWith:
                parameters.Add(new NpgsqlParameter(pName, filter.Value?.ToString() ?? ""));
                return $"({propAccess}) LIKE @{pName} || '%'";

            case FilterOperator.NotEqual:
                if (filter.Value == null)
                {
                    return $"({propAccess}) IS NOT NULL";
                }
                parameters.Add(new NpgsqlParameter(pName, filter.Value.ToString()!));
                return $"({propAccess}) != @{pName}";

            default:
                throw new ArgumentException($"Unsupported filter operator for nested array element: {filter.Operator}");
        }
    }

    private static string BuildPropUpdateSql(
        string baseExpr, string elemAlias, PropertyUpdate update,
        List<NpgsqlParameter> parameters, ref int paramIdx)
    {
        switch (update.UpdateType)
        {
            case PropertyUpdateType.Set:
            {
                var pName = $"pus{paramIdx}";
                paramIdx++;
                if (update.Value == null)
                {
                    return $"jsonb_set({baseExpr}, '{{{update.PropertyName}}}', 'null'::jsonb)";
                }
                parameters.Add(new NpgsqlParameter(pName, update.Value.ToString()!));
                return $"jsonb_set({baseExpr}, '{{{update.PropertyName}}}', to_jsonb(@{pName}::text))";
            }

            case PropertyUpdateType.ReplacePrefix:
            {
                var oldP = $"puo{paramIdx}";
                var newP = $"pun{paramIdx}";
                paramIdx++;
                parameters.Add(new NpgsqlParameter(oldP, update.OldPrefix ?? ""));
                parameters.Add(new NpgsqlParameter(newP, update.NewPrefix ?? ""));
                return $"jsonb_set({baseExpr}, '{{{update.PropertyName}}}', " +
                       $"to_jsonb(@{newP} || substr({elemAlias}->>'{update.PropertyName}', length(@{oldP}) + 1)))";
            }

            default:
                throw new ArgumentException($"Unsupported property update type: {update.UpdateType}");
        }
    }

    private QueryChunk ConstructOneConditionFilter(Filter filter, ProjectionDocumentSchema schema)
    {
        var queryChunk = new QueryChunk();
        
        var filterOperator = "";
        var propertyName = filter.PropertyName;
        var propertyParameterName = filter.PropertyName;

        if (string.IsNullOrEmpty(propertyName) || propertyName == "*")
        {
            return queryChunk;
        }

        var nestedPath = propertyName.Split('.');

        var propertySchema = schema.Properties.FirstOrDefault(p => p.PropertyName == nestedPath.First());
        if (propertySchema == null)
        {
            throw new ProjectionQueryFilterException(propertyName, schema.SchemaName);
        }
        
        // Nested array check.
        // From query perspective both nested object and nested array item lookup look same: user.id = 1 or users.id = 1
        // so we need to use schema definition to find out whether it's an array. Because for arrays the query will be completely different. 
        var isArray = propertySchema.IsNestedArray;

        if (nestedPath.Length > 1)
        {
            propertySchema = propertySchema.NestedObjectProperties.First(p => p.PropertyName == nestedPath[1]);
            
            if (isArray == true)
            {
                // TODO: it's only working with one level of depth right now :(
                var fromSelect = $"jsonb_array_elements({nestedPath.First()}) with ordinality " +
                                 $"{nestedPath.First()}_array({nestedPath.First()}_array_item, position)";
                queryChunk.AdditionalFromSelects.Add(fromSelect);    
                propertyName = $"{nestedPath.First()}_array.{nestedPath.First()}_array_item->>{string.Join("->>", nestedPath.Skip(1).Select(n => $"'{n}'"))}";
            }
            else
            {
                propertyName = $"{nestedPath.First()}->>{string.Join("->>", nestedPath.Skip(1).Select(n => $"'{n}'"))}";
            }

            propertyParameterName = string.Join("_", nestedPath);
        }

        switch (filter.Operator)
        {
            case FilterOperator.Equal:
                filterOperator = filter.Value == null ? "IS" : "=";
                break;
            case FilterOperator.NotEqual:
                filterOperator = filter.Value == null ? "IS NOT" : "!=";
                break;
            case FilterOperator.Greater:
                filterOperator = ">";
                break;
            case FilterOperator.GreaterOrEqual:
                filterOperator = ">=";
                break;
            case FilterOperator.Lower:
                filterOperator = "<";
                break;
            case FilterOperator.LowerOrEqual:
                filterOperator = "<=";
                break;
            case FilterOperator.StartsWith:
            case FilterOperator.EndsWith:
            case FilterOperator.Contains:
                filterOperator = "LIKE";
                break;
            case FilterOperator.StartsWithIgnoreCase:
            case FilterOperator.EndsWithIgnoreCase:
            case FilterOperator.ContainsIgnoreCase:
                filterOperator = "ILIKE";
                break;
            case FilterOperator.ArrayContains:
                filterOperator = "?";
                break;
            default:
                throw new ArgumentException($"Unsupported filter operator: {filter.Operator}");
        }
        
        var npgsqlParameter = new NpgsqlParameter(propertyParameterName, filter.Value ?? DBNull.Value);

        if (filter.Value is Guid)
        {
            propertyName = $"({propertyName})::uuid";
        } 
        else if (propertySchema.PropertyType == TypeCode.DateTime)
        {
            propertyName = $"({propertyName})::timestamp with time zone";
            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.TimestampTz;
        }
        else if (filter.Value is int)
        {
            propertyName = $"({propertyName})::int";
        }
        else if (filter.Value is long)
        {
            propertyName = $"({propertyName})::bigint";
        }
        else if (filter.Value is decimal)
        {
            propertyName = $"({propertyName})::decimal";
        }
        else if (filter.Value is float)
        {
            propertyName = $"({propertyName})::float";
        }
        
        queryChunk.WhereChunk = $"{propertyName} {filterOperator} ";

        if (filter.Value == null)
        {
            queryChunk.WhereChunk += "NULL";
        } 
        else
        {
            switch (filter.Operator)
            {
                case FilterOperator.StartsWithIgnoreCase:
                case FilterOperator.StartsWith:
                    queryChunk.WhereChunk += $"@{propertyParameterName} || '%'";
                    break;
                case FilterOperator.EndsWithIgnoreCase:
                case FilterOperator.EndsWith:
                    queryChunk.WhereChunk += $"'%' || @{propertyParameterName}";
                    break;
                case FilterOperator.ContainsIgnoreCase:
                case FilterOperator.Contains:
                    if (isArray == true)
                    {
                        // This is due to the fact that we don't always have access to the schema when building filters.
                        // For example, when generating linq expressions from filters it's not possible to know whether filterable property is array or string.
                        // Hence the need to have separate operators - `Contains` for strings and `ArrayContains` for arrays.  
                        throw new ArgumentException("Please use ArrayContains instead.");
                    }

                    queryChunk.WhereChunk += $"'%' || @{propertyParameterName} || '%'";
                    break;
                default:
                    queryChunk.WhereChunk += $"@{propertyParameterName}";
                    break;
            }

            queryChunk.Parameters.Add(npgsqlParameter);
        }

        return queryChunk;
    }

    private QueryChunk ConstructConditionFilter(Filter filter, ProjectionDocumentSchema schema)
    {
        var queryChunk = new QueryChunk();

        var q = ConstructOneConditionFilter(filter, schema);

        queryChunk.WhereChunk += q.WhereChunk;
        queryChunk.Parameters.AddRange(q.Parameters);
        queryChunk.AdditionalFromSelects.AddRange(q.AdditionalFromSelects);

        foreach (var f in filter.Filters)
        {
            if (!string.IsNullOrEmpty(queryChunk.WhereChunk))
            {
                queryChunk.WhereChunk += $" {f.Logic} ";
            }

            var wrapWithParentheses = f.Filter.Filters.Count > 0;

            if (wrapWithParentheses)
            {
                queryChunk.WhereChunk += "(";
            }

            var innerFilterQueryChunk = ConstructConditionFilter(f.Filter, schema);
            
            // need to go through all innerFilter parameters to see if we don't already have ones with same names
            // and replace them with additional suffix if needed
            foreach (var innerFilterParameter in innerFilterQueryChunk.Parameters)
            {
                while (queryChunk.Parameters.Any(p => p.ParameterName == innerFilterParameter.ParameterName))
                {
                    var newParameterName = innerFilterParameter.ParameterName;
                    
                    var parameterNumberMatch = new Regex(".*(_\\d+)$").Match(innerFilterParameter.ParameterName);

                    if (parameterNumberMatch.Success)
                    {
                        var number = int.Parse(parameterNumberMatch.Groups[1].Value.Replace("_", ""));
                        newParameterName = newParameterName.Replace(parameterNumberMatch.Groups[1].Value, $"_{number + 1}");
                    }
                    else
                    {
                        newParameterName = newParameterName + "_1";
                    }
                    
                    innerFilterQueryChunk.WhereChunk = innerFilterQueryChunk.WhereChunk.Replace($"@{innerFilterParameter.ParameterName}", $"@{newParameterName}");
                    innerFilterParameter.ParameterName = newParameterName;
                }
            }
            
            queryChunk.WhereChunk += innerFilterQueryChunk.WhereChunk;
            queryChunk.Parameters.AddRange(innerFilterQueryChunk.Parameters);

            if (wrapWithParentheses)
            {
                queryChunk.WhereChunk += ")";
            }
        }

        return queryChunk;
    }

    private QueryChunk ConstructConditionFilters(List<Filter> filters, ProjectionDocumentSchema schema)
    {
        var queryChunk = new QueryChunk();

        var whereClauses = new List<string>();

        foreach (var f in filters)
        {
            var filterQueryChunk = ConstructConditionFilter(f, schema);

            // Deduplicate parameter names across top-level filters
            foreach (var filterParameter in filterQueryChunk.Parameters)
            {
                while (queryChunk.Parameters.Any(p => p.ParameterName == filterParameter.ParameterName))
                {
                    var newParameterName = filterParameter.ParameterName;

                    var parameterNumberMatch = new Regex(".*(_\\d+)$").Match(filterParameter.ParameterName);

                    if (parameterNumberMatch.Success)
                    {
                        var number = int.Parse(parameterNumberMatch.Groups[1].Value.Replace("_", ""));
                        newParameterName = newParameterName.Replace(parameterNumberMatch.Groups[1].Value, $"_{number + 1}");
                    }
                    else
                    {
                        newParameterName = newParameterName + "_1";
                    }

                    filterQueryChunk.WhereChunk = filterQueryChunk.WhereChunk.Replace($"@{filterParameter.ParameterName}", $"@{newParameterName}");
                    filterParameter.ParameterName = newParameterName;
                }
            }

            whereClauses.Add($"({filterQueryChunk.WhereChunk})");
            queryChunk.Parameters.AddRange(filterQueryChunk.Parameters);
            //Don't add duplicates
            queryChunk.AdditionalFromSelects.AddRange(filterQueryChunk.AdditionalFromSelects
                .Except(queryChunk.AdditionalFromSelects));
        }

        queryChunk.WhereChunk = string.Join(" AND ", whereClauses);
        return queryChunk;
    }

    private (string, List<NpgsqlParameter>) ConstructSearchQuery(string searchText, ProjectionDocumentSchema schema)
    {
        var words = searchText.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var parameters = new List<NpgsqlParameter>();

        var fieldTemplates = new List<string>();
        var aliasCounter = 0;
        CollectSearchableFieldTemplates(schema.Properties, fieldTemplates, parentPath: null, isParentArray: false, ref aliasCounter);

        if (fieldTemplates.Count == 0)
        {
            return ("TRUE", parameters);
        }

        var wordClauses = new List<string>();
        for (var i = 0; i < words.Length; i++)
        {
            var paramName = $"searchWord{i}";
            parameters.Add(new NpgsqlParameter(paramName, $"%{words[i]}%"));

            var fieldMatches = fieldTemplates
                .Select(t => t.Replace("{PARAM}", $"@{paramName}"))
                .ToList();

            wordClauses.Add($"({string.Join(" OR ", fieldMatches)})");
        }

        return ($"({string.Join(" AND ", wordClauses)})", parameters);
    }

    private static void CollectSearchableFieldTemplates(
        List<ProjectionDocumentPropertySchema> properties,
        List<string> templates,
        string? parentPath,
        bool isParentArray,
        ref int aliasCounter)
    {
        foreach (var property in properties)
        {
            if (property.IsNestedObject && property.NestedObjectProperties != null)
            {
                var path = parentPath ?? property.PropertyName;
                CollectSearchableFieldTemplates(property.NestedObjectProperties, templates, path, false, ref aliasCounter);
            }
            else if (property.IsNestedArray && property.NestedObjectProperties != null)
            {
                var path = parentPath ?? property.PropertyName;
                CollectSearchableFieldTemplates(property.NestedObjectProperties, templates, path, true, ref aliasCounter);
            }
            else if (property.IsSearchable)
            {
                if (parentPath != null)
                {
                    if (isParentArray)
                    {
                        var alias = $"_se{aliasCounter++}";
                        templates.Add(
                            $"EXISTS(SELECT 1 FROM jsonb_array_elements({parentPath}) {alias} " +
                            $"WHERE {alias}->>'{property.PropertyName}' ILIKE {{PARAM}})"
                        );
                    }
                    else
                    {
                        templates.Add($"{parentPath}->>'{property.PropertyName}' ILIKE {{PARAM}}");
                    }
                }
                else
                {
                    templates.Add($"{property.PropertyName} ILIKE {{PARAM}}");
                }
            }
        }
    }

    private static string ConstructCreateTableSql(string tableName, ProjectionDocumentSchema schema)
    {
        var sb = new StringBuilder();
        sb.AppendFormat("CREATE TABLE \"{0}\" (", tableName);

        var columnsSql = schema.Properties
            .Select(ConstructColumnCreateStatementForProperty);

        sb.Append(string.Join(',', columnsSql));
        sb.Append(')');

        return sb.ToString();
    }

    private static string ConstructCreateIndexesSql(string tableName, ProjectionDocumentSchema schema)
    {
        var sb = new StringBuilder();

        foreach (var property in schema.Properties)
        {
            if (property.IsKey)
            {
                continue;
            }

            if (property.IsNestedObject || property.IsNestedArray)
            {
                sb.Append(
                    $"CREATE INDEX IF NOT EXISTS \"ix_{tableName}_{property.PropertyName}_gin\"" +
                    $" ON \"{tableName}\" USING GIN ({property.PropertyName});"
                );
            }
            else if (property.IsFilterable)
            {
                sb.Append(
                    $"CREATE INDEX IF NOT EXISTS \"ix_{tableName}_{property.PropertyName}\"" +
                    $" ON \"{tableName}\" ({property.PropertyName});"
                );
            }
        }

        return sb.ToString();
    }

    private static string ConstructColumnCreateStatementForProperty(ProjectionDocumentPropertySchema property)
    {
        string? column;

        if (property.IsNestedObject || property.IsNestedArray)
        {
            column = $"{property.PropertyName} jsonb";
        }
        else
        {
            string? columnType;

            columnType = property.PropertyType switch
            {
                TypeCode.Int32 => "integer",
                TypeCode.Int64 => "bigint",
                TypeCode.Single or TypeCode.Decimal => "decimal",
                TypeCode.Double => "double precision",
                TypeCode.Boolean => "boolean",
                TypeCode.String => "text",
                TypeCode.Object => "uuid",
                // var elementType = propertyType.GetElementType();
                // if (Type.GetTypeCode(elementType) != TypeCode.String)
                // {
                //     throw new Exception("Unsupported array element type!");
                // }
                //
                // fieldType = "jsonb";
                //
                // break;
                TypeCode.DateTime => "timestamp with time zone", // This does not mean it stores timezone information, it only stores UTC,
                                                                 // read more here: https://www.npgsql.org/doc/types/datetime.html#timestamps-and-timezones
                _ => throw new Exception(
                    $"Postgresql Projection Repository provider doesn't support type {property.PropertyType} for property {property.PropertyName}."
                ),
            };
            column = $"{property.PropertyName} {columnType}";

            if (property.IsKey)
            {
                column += " NOT NULL PRIMARY KEY";
            }
        }

        return column;
    }
}