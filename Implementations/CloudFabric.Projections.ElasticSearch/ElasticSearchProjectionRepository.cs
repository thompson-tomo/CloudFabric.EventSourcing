using System.Collections;
using System.Text.Json;
using CloudFabric.Projections.ElasticSearch.Helpers;
using CloudFabric.Projections.Queries;
using CloudFabric.Projections.Utils;
using Elasticsearch.Net;
using Microsoft.Extensions.Logging;
using Nest;
using SortOrder = Nest.SortOrder;

namespace CloudFabric.Projections.ElasticSearch;

public class ElasticSearchProjectionRepository<TProjectionDocument> : ElasticSearchProjectionRepository, IProjectionRepository<TProjectionDocument>
    where TProjectionDocument : ProjectionDocument
{
    public ElasticSearchProjectionRepository(
        ElasticSearchBasicAuthConnectionSettings basicAuthConnectionSettings,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming
    ) : base(
        basicAuthConnectionSettings,
        ProjectionDocumentSchemaFactory.FromTypeWithAttributes<TProjectionDocument>(),
        loggerFactory,
        disableRequestStreaming
    )
    {
    }

    public ElasticSearchProjectionRepository(
        ElasticSearchApiKeyAuthConnectionSettings apiKeyAuthConnectionSettings,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming
    ) : base(
        apiKeyAuthConnectionSettings,
        ProjectionDocumentSchemaFactory.FromTypeWithAttributes<TProjectionDocument>(),
        loggerFactory,
        disableRequestStreaming
    )
    {
    }

    public new async Task<TProjectionDocument?> Single(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    )
    {
        var document = await base.Single(id, partitionKey, cancellationToken, indexSelector);

        if (document == null)
        {
            return null;
        }

        return ProjectionDocumentSerializer.DeserializeFromDictionary<TProjectionDocument>(document);
    }

    public Task Upsert(
        TProjectionDocument document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    )
    {
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
        var result = await base.Query(projectionQuery, partitionKey, cancellationToken, indexSelector);

        var records = new List<QueryResultDocument<TProjectionDocument>>();

        foreach (var doc in result.Records)
        {
            if (doc.Document != null)
            {
                records.Add(
                    new QueryResultDocument<TProjectionDocument>
                    {
                        Document = ProjectionDocumentSerializer.DeserializeFromDictionary<TProjectionDocument>(doc.Document)
                    }
                );
            }
        }

        return new ProjectionQueryResult<TProjectionDocument>
        {
            IndexName = result.IndexName,
            TotalRecordsFound = result.TotalRecordsFound,
            Records = records,
            DebugInformation = result.DebugInformation
        };
    }
}

public class ElasticSearchProjectionRepository : ProjectionRepository
{
    private readonly ProjectionDocumentSchema _projectionDocumentSchema;
    private string? _keyPropertyName;
    private readonly ElasticClient _client;
    private readonly ElasticSearchIndexer _indexer;
    private readonly ILogger<ElasticSearchProjectionRepository> _logger;
    private readonly Task _clusterSettingsTask;

    /// <summary>
    /// When request streaming is disabled, elastic adds debug information about request and response to response object which can
    /// be useful when troubleshooting search problems.
    /// </summary>
    private readonly bool _disableRequestStreaming;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="apiKeyAuthConnectionSettings"></param>
    /// <param name="projectionDocumentSchema"></param>
    /// <param name="loggerFactory"></param>
    /// <param name="disableRequestStreaming">
    /// When request streaming is disabled, elastic adds debug information about request and response to response object which can
    /// be useful when troubleshooting search problems.
    ///
    /// Defaults to false to improve performance.
    /// </param>
    public ElasticSearchProjectionRepository(
        ElasticSearchApiKeyAuthConnectionSettings apiKeyAuthConnectionSettings,
        ProjectionDocumentSchema projectionDocumentSchema,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming = false
    ) : base(projectionDocumentSchema, loggerFactory.CreateLogger<ProjectionRepository>())
    {
        _projectionDocumentSchema = projectionDocumentSchema;
        _logger = loggerFactory.CreateLogger<ElasticSearchProjectionRepository>();
        _disableRequestStreaming = disableRequestStreaming;

        var connectionSettings = new ConnectionSettings(
                apiKeyAuthConnectionSettings.CloudId, new ApiKeyAuthenticationCredentials(
                    apiKeyAuthConnectionSettings.ApiKeyId,
                    apiKeyAuthConnectionSettings.ApiKey
                )
            )
            .ThrowExceptions()
            .DefaultFieldNameInferrer(x => x);

        _client = new ElasticClient(connectionSettings);

        // Very important setting - when we remove the index, system has to create it explicitly, with all custom analyzers and settings.
        // Otherwise the index won't have proper attributes and just won't work.
        // Stored as Task and awaited in CreateIndex to ensure settings are applied before index creation.
        _clusterSettingsTask = _client.Cluster.PutSettingsAsync(settings => settings.Persistent(p => {
            p["action.auto_create_index"] = "false";
            return p;
        }));

        _indexer = new ElasticSearchIndexer(apiKeyAuthConnectionSettings, loggerFactory);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="basicAuthConnectionSettings"></param>
    /// <param name="projectionDocumentSchema"></param>
    /// <param name="loggerFactory"></param>
    /// <param name="disableRequestStreaming">
    /// When request streaming is disabled, elastic adds debug information about request and response to response object which can
    /// be useful when troubleshooting search problems.
    ///
    /// Defaults to false to improve performance.
    /// </param>
    public ElasticSearchProjectionRepository(
        ElasticSearchBasicAuthConnectionSettings basicAuthConnectionSettings,
        ProjectionDocumentSchema projectionDocumentSchema,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming = false
    ) : base(projectionDocumentSchema, loggerFactory.CreateLogger<ProjectionRepository>())
    {
        _projectionDocumentSchema = projectionDocumentSchema;
        _logger = loggerFactory.CreateLogger<ElasticSearchProjectionRepository>();
        _disableRequestStreaming = disableRequestStreaming;

        var connectionSettings = new ConnectionSettings(new Uri(basicAuthConnectionSettings.Uri))
            .BasicAuthentication(basicAuthConnectionSettings.Username, basicAuthConnectionSettings.Password)
            .CertificateFingerprint(basicAuthConnectionSettings.CertificateThumbprint)
            .ThrowExceptions()
            // means that we do not change property names when indexing (like pascal case to camel case)
            .DefaultFieldNameInferrer(x => x);

        _client = new ElasticClient(connectionSettings);

        // Very important setting - when we remove the index, system has to create it explicitly, with all custom analyzers and settings.
        // Otherwise the index won't have proper attributes and just won't work.
        // Stored as Task and awaited in CreateIndex to ensure settings are applied before index creation.
        _clusterSettingsTask = _client.Cluster.PutSettingsAsync(settings => settings.Persistent(p => {
            p["action.auto_create_index"] = "false";
            return p;
        }));

        _indexer = new ElasticSearchIndexer(basicAuthConnectionSettings, loggerFactory);
    }

    public string? KeyColumnName
    {
        get
        {
            if (string.IsNullOrEmpty(_keyPropertyName))
            {
                _keyPropertyName = _projectionDocumentSchema.KeyColumnName;
            }

            return _keyPropertyName;
        }
    }
  
    protected override async Task CreateIndex(string indexName, ProjectionDocumentSchema projectionDocumentSchema)
    {
        await _clusterSettingsTask;
        await _indexer.CreateOrUpdateIndex(indexName, projectionDocumentSchema);
    }

    protected override async Task DropIndex(string indexName, CancellationToken cancellationToken = default)
    {
        await _client.Indices.DeleteAsync(new DeleteIndexRequest(indexName), cancellationToken);
    }

    public override async Task<Dictionary<string, object?>?> Single(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    )
    {
        var indexDescriptor = await GetIndexDescriptorForOperation(indexSelector, cancellationToken);

        try
        {
            var item = await _client.GetAsync<Dictionary<string, object?>>(
                id,
                x => x.Index(indexDescriptor.IndexName).Routing(partitionKey),
                ct: cancellationToken
            );

            if (item?.Source == null)
            {
                return null;
            }

            return DeserializeDictionary(item.Source);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to retrieve document with {@Id} ({@Index})", id, indexDescriptor
            );

            throw;
        }
    }

    public override async Task Delete(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    )
    {
        var indexDescriptor = await GetIndexDescriptorForOperation(indexSelector, cancellationToken);

        try
        {
            await _client.DeleteAsync(
                new DeleteRequest(indexDescriptor.IndexName, id)
                {
                    Routing = new Routing(partitionKey)
                },
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to delete Document with {@Id} ({@Index})", id, indexDescriptor
            );

            throw;
        }
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
        
        foreach (var indexStatus in indexState.IndexesStatuses)
        {
            try
            {
                if (partitionKey == null)
                {
                    await _client.Indices.DeleteAsync(new DeleteIndexRequest(indexStatus.IndexName), cancellationToken);
                }
                else
                {
                    await _client.DeleteByQueryAsync<Dictionary<string, object?>>(
                        x => x.Query(
                                q => q.Bool(
                                    b => new BoolQuery
                                    {
                                        Filter = new List<QueryContainer>
                                        {
                                            new QueryStringQuery() { Query = $"{nameof(partitionKey)}:{partitionKey}" }
                                        }
                                    }
                                )
                            )
                            .Routing(partitionKey),
                        cancellationToken
                    );
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete index {@Index}", indexStatus.IndexName);
                throw;
            }
        }
        
        indexState.IndexesStatuses.Clear();
        await SaveProjectionIndexState(indexState);
        //await _client.Indices.DeleteAsync(new DeleteIndexRequest(PROJECTION_INDEX_STATE_INDEX_NAME), cancellationToken);
    }

    protected override async Task UpsertInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default)
    {
        try
        {
            document.TryGetValue("Id", out object? id);
            document[nameof(ProjectionDocument.PartitionKey)] = partitionKey;
            document[nameof(ProjectionDocument.UpdatedAt)] = updatedAt;

            await _client.IndexAsync(
                new IndexRequest<Dictionary<string, object?>>(document, indexDescriptor.IndexName, id: id?.ToString())
                {
                    Routing = new Routing(partitionKey),
                    Refresh = Refresh.False,
                    RequestConfiguration = new RequestConfiguration() {
                        ThrowExceptions = true
                    }
                },
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to upsert Document with {@Id} ({@Index})", document[_projectionDocumentSchema.KeyColumnName], indexDescriptor.IndexName
            );

            throw;
        }
    }

    protected override async Task<ProjectionQueryResult<Dictionary<string, object?>>> QueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default
    ) {
        try
        {
            var result = await _client.SearchAsync<Dictionary<string, object?>>(
                request =>
                {
                    request = request.Index(indexDescriptor.IndexName);

                    request = request.TrackTotalHits();
                    request = request.Query(q => ConstructSearchQuery(q, projectionQuery));
                    request = request.Sort(s => ConstructSort(s, projectionQuery));
                    request = request.Skip(projectionQuery.Offset);

                    if (projectionQuery.Limit.HasValue)
                    {
                        request = request.Take(projectionQuery.Limit.Value);
                    }

                    if (!string.IsNullOrEmpty(partitionKey))
                    {
                        request = request.Routing(partitionKey);
                    }

                    if (_disableRequestStreaming)
                    {
                        request = request.RequestConfiguration(conf => conf.DisableDirectStreaming());
                    }

                    return request;
                }
            );

            // if (result.ApiCall.HttpStatusCode == 404 && indexDescriptor.IndexName == PROJECTION_INDEX_STATE_INDEX_NAME)
            // {
            //     // usually parent class receives InvalidSchemaException when saving projection index state
            //     // but that doesn't work for elasticsearch since it's .Index method never returns errors - it simply queues the operation
            //     await CreateIndex(
            //         PROJECTION_INDEX_STATE_INDEX_NAME,
            //         ProjectionIndexStateSchema
            //     );
            // }

            return new ProjectionQueryResult<Dictionary<string, object?>>
            {
                IndexName = indexDescriptor.IndexName,
                TotalRecordsFound = (int)result.Total,
                Records = result.Documents.Select(
                        x =>
                            new QueryResultDocument<Dictionary<string, object?>>
                            {
                                Document = DeserializeDictionary(x)
                            }
                    )
                    .ToList(),
                DebugInformation = result.DebugInformation
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error querying index ({@Index})", indexDescriptor.IndexName
            );

            throw;
        }
    }
    
    private Dictionary<string, object?> DeserializeDictionary(Dictionary<string, object?> document)
    {
        var newDictionary = new Dictionary<string, object?>(document);

        foreach (var kv in newDictionary)
        {
            var propertySchema = _projectionDocumentSchema.Properties.FirstOrDefault(p => p.PropertyName == kv.Key);

            if (propertySchema != null)
            {
                try
                {
                    newDictionary[kv.Key] = DeserializeDictionaryItem(kv.Value, propertySchema);
                }
                catch (Exception ex)
                {
                    throw new Exception(
                        $"Failed to deserialize dictionary item for property {propertySchema.PropertyName}:{propertySchema.PropertyType}, " +
                        $"value: {kv.Value}", ex
                    );
                }
            }
        }

        return newDictionary;
    }
    
    public override async Task SaveProjectionIndexState(ProjectionIndexState state)
    {
        await CreateIndex(
            PROJECTION_INDEX_STATE_INDEX_NAME,
            ProjectionIndexStateSchema
        );
        
        var document = ProjectionDocumentSerializer.SerializeToDictionary(state);
        document.TryGetValue("Id", out object? id);
        document[nameof(ProjectionDocument.PartitionKey)] = PROJECTION_INDEX_STATE_INDEX_NAME;
        document[nameof(ProjectionDocument.UpdatedAt)] = state.UpdatedAt;

        try
        {
            await _client.IndexAsync(
                new IndexRequest<Dictionary<string, object?>>(document, PROJECTION_INDEX_STATE_INDEX_NAME, id: id?.ToString())
                {
                    Routing = new Routing(PROJECTION_INDEX_STATE_INDEX_NAME),
                    Refresh = Refresh.True,
                    RequestConfiguration = new RequestConfiguration() {
                        ThrowExceptions = true
                    }
                }
            );
        }
        catch (Exception ex)
        {
            try
            {
                await CreateIndex(
                    PROJECTION_INDEX_STATE_INDEX_NAME,
                    ProjectionIndexStateSchema
                );
                
                await SaveProjectionIndexState(state);
            }
            catch (Exception createTableException)
            {
                var exception = new Exception($"Failed to create a table for projection \"{PROJECTION_INDEX_STATE_INDEX_NAME}\"", createTableException);
                throw exception;
            }
        }
    }

    private object? DeserializeDictionaryItem(object? item, ProjectionDocumentPropertySchema propertySchema)
    {
        if (item == null)
        {
            return null;
        }
        else if (item is JsonElement valueAsJsonElement)
        {
            return JsonToObjectConverter.Convert(valueAsJsonElement, propertySchema);
        }
        else if (propertySchema.PropertyType == TypeCode.Object && item is string)
        {
            return Guid.Parse(item as string);
        }
        // ES Nest returns int as long for some reason
        else if (propertySchema.PropertyType == TypeCode.Int32)
        {
            return Convert.ToInt32(item);
        }
        else if (propertySchema.PropertyType == TypeCode.Decimal)
        {
            return Convert.ToDecimal(item);
        }
        else if (propertySchema.IsNestedObject)
        {
            var nestedObject = item as Dictionary<string, object?>;
            foreach (var nestedProperty in nestedObject)
            {
                var nestedPropertySchema = propertySchema.NestedObjectProperties.First(x => x.PropertyName == nestedProperty.Key);
                nestedObject[nestedProperty.Key] = DeserializeDictionaryItem(nestedProperty.Value, nestedPropertySchema);
            }

            return nestedObject;
        }
        else if (propertySchema.IsNestedArray && propertySchema.ArrayElementType == TypeCode.Object)
        {
            IList<object?> resultList = new List<object?>();
            foreach (var listItem in (item as IList))
            {
                if (listItem is string listItemString)
                {
                    resultList.Add(Guid.Parse(listItemString));
                }
                else
                {
                    var listItemDictionary = listItem as Dictionary<string, object?>;
                    foreach (var listItemProperty in listItemDictionary)
                    {
                        listItemDictionary[listItemProperty.Key] = DeserializeDictionaryItem(
                            listItemProperty.Value,
                            propertySchema.NestedObjectProperties.First(x => x.PropertyName == listItemProperty.Key)
                        );
                    }

                    resultList.Add(listItemDictionary);
                }
            }

            return resultList;
        }

        return item;
    }

    private QueryContainer ConstructSearchQuery<T>(QueryContainerDescriptor<T> searchDescriptor, ProjectionQuery projectionQuery) where T : class
    {
        // construct search query
        List<QueryContainer> textQueries = new();

        if (!string.IsNullOrWhiteSpace(projectionQuery.SearchText) && projectionQuery.SearchText != "*")
        {
            var queries = ElasticSearchQueryFactory.ConstructSearchQuery(_projectionDocumentSchema, projectionQuery.SearchText);

            textQueries.Add(
                new BoolQuery()
                {
                    Should = queries
                }
            );
        }

        if (projectionQuery.Filters == null || !projectionQuery.Filters.Any())
        {
            return searchDescriptor.Bool(
                q =>
                    new BoolQuery()
                    {
                        Should = textQueries
                    }
            );
        }

        // construct filters
        var filters = ElasticSearchFilterFactory.ConstructFilters(projectionQuery.Filters);

        return searchDescriptor.Bool(
            q =>
                q.Must(
                    new BoolQuery
                    {
                        Should = textQueries
                    },
                    new BoolQuery
                    {
                        Filter = filters
                    }
                )
        );
    }

    private SortDescriptor<T> ConstructSort<T>(SortDescriptor<T> sortDescriptor, ProjectionQuery projectionQuery) where T : class
    {
        foreach (var orderBy in projectionQuery.OrderBy)
        {
            var keyPaths = orderBy.KeyPath.Split('.');
            SortOrder sortOrder = orderBy.Order.ToLower() == "asc" ? SortOrder.Ascending : SortOrder.Descending;

            sortDescriptor = sortDescriptor.Field(
                field =>
                {
                    var descriptor = field
                        .Field(new Field(orderBy.KeyPath))
                        .Order(sortOrder);

                    // add sorting statement for each nested level
                    for (var pathElementsNumber = 1; pathElementsNumber < keyPaths.Length; pathElementsNumber++)
                    {
                        descriptor = descriptor
                            .Nested(
                                nested =>
                                {
                                    string nestedPath = string.Join('.', keyPaths.Take(pathElementsNumber));

                                    var nestedDescriptor = nested.Path(nestedPath);

                                    // check property filters (for example if we sort by an array element, we need to filter it)
                                    if (orderBy.Filters.Any())
                                    {
                                        // take parent path for current nested level
                                        string parentObjectPath = nestedPath.Contains(".")
                                            ? nestedPath.Substring(0, nestedPath.LastIndexOf('.'))
                                            : nestedPath;

                                        // find a filter with the same parent path
                                        // which means to filter by a property of the same object
                                        var filter = orderBy.Filters.FirstOrDefault(
                                            x =>
                                            {
                                                string filterParentPath = x.FilterKeyPath.Contains(".")
                                                    ? x.FilterKeyPath.Substring(0, x.FilterKeyPath.LastIndexOf('.'))
                                                    : x.FilterKeyPath;

                                                return filterParentPath == parentObjectPath;
                                            }
                                        );

                                        if (filter != null)
                                        {
                                            nestedDescriptor.Filter(
                                                f => f
                                                    .Term(
                                                        term => term
                                                            .Field(filter.FilterKeyPath)
                                                            .Value(filter.FilterValue)
                                                    )
                                            );
                                        }
                                    }

                                    return nestedDescriptor;
                                }
                            );
                    }

                    return descriptor;
                }
            );
        }

        return sortDescriptor;
    }
}