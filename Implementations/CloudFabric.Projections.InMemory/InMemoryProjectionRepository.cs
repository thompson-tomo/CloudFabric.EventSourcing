using System.Collections.Concurrent;
using CloudFabric.Projections.Queries;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections.InMemory;

public class InMemoryProjectionRepository<TProjectionDocument>
    : InMemoryProjectionRepository, IProjectionRepository<TProjectionDocument>
    where TProjectionDocument : ProjectionDocument
{
    public InMemoryProjectionRepository(
        ConcurrentDictionary<string, ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>> storage,
        ILoggerFactory loggerFactory
    ) : base(ProjectionDocumentSchemaFactory.FromTypeWithAttributes<TProjectionDocument>(), storage, loggerFactory)
    {
    }

    public new async Task<TProjectionDocument?> Single(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    ) {
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
    ) {
        var documentDictionary = ProjectionDocumentSerializer.SerializeToDictionary(document);
        return Upsert(documentDictionary, partitionKey, updatedAt, cancellationToken, indexSelector);
    }

    public new async Task<ProjectionQueryResult<TProjectionDocument>> Query(
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    ) {
        ProjectionQueryResult<Dictionary<string, object?>> recordsDictionary = await base.Query(projectionQuery, partitionKey, cancellationToken);

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
            IndexName = recordsDictionary.IndexName,
            TotalRecordsFound = recordsDictionary.TotalRecordsFound,
            Records = records
        };
    }
}

public class InMemoryProjectionRepository : ProjectionRepository
{
    private readonly ProjectionDocumentSchema _projectionDocumentSchema;

    /// <summary>
    /// Data storage
    /// </summary>                     Index name         Item Id and PartitionKey                     Item properties and values
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>> _storage;

    public InMemoryProjectionRepository(
        ProjectionDocumentSchema projectionDocumentSchema,
        ConcurrentDictionary<string, ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>> storage,
        ILoggerFactory loggerFactory
    ) : base(projectionDocumentSchema, loggerFactory.CreateLogger<ProjectionRepository>())
    {
        _projectionDocumentSchema = projectionDocumentSchema;
        _storage = storage;

        _storage.TryAdd(PROJECTION_INDEX_STATE_INDEX_NAME, new ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>());
    }

    protected override Task CreateIndex(string indexName, ProjectionDocumentSchema projectionDocumentSchema)
    {
        _storage.TryAdd(indexName, new ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>());

        return Task.CompletedTask;
    }

    protected override Task DropIndex(string indexName, CancellationToken cancellationToken = default)
    {
        _storage.TryRemove(indexName, out _);
        return Task.CompletedTask;
    }

    protected override Task<Dictionary<string, object?>?> SingleInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    ) {
        if (!_storage.TryGetValue(indexDescriptor.IndexName, out var storage))
        {
            return Task.FromResult<Dictionary<string, object?>?>(null);
        }

        if (storage.TryGetValue((id.ToString(), partitionKey), out var document))
        {
            return Task.FromResult<Dictionary<string, object?>?>(new Dictionary<string, object?>(document));
        }

        return Task.FromResult<Dictionary<string, object?>?>(null);
    }

    protected override Task DeleteInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    ) {
        if (_storage.TryGetValue(indexDescriptor.IndexName, out var storage))
        {
            storage.TryRemove((id.ToString(), partitionKey), out _);
        }

        return Task.CompletedTask;
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
            if (!_storage.TryGetValue(indexStatus.IndexName, out var storage))
            {
                continue;
            }

            if (partitionKey == null)
            {
                storage.Clear();
            }
            else
            {
                var keysToRemove = storage.Keys.Where(k => k.PartitionKey == partitionKey).ToList();

                foreach (var key in keysToRemove)
                {
                    storage.TryRemove(key, out _);
                }
            }
        }

        indexState.IndexesStatuses.Clear();
        await SaveProjectionIndexState(indexState);
    }

    protected override Task UpsertInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    ) {
        var keyValue = document[indexDescriptor.ProjectionDocumentSchema.KeyColumnName];
        if (keyValue == null)
        {
            throw new ArgumentException("document.Id could not be null", indexDescriptor.ProjectionDocumentSchema.KeyColumnName);
        }

        document[nameof(ProjectionDocument.PartitionKey)] = partitionKey;
        document[nameof(ProjectionDocument.UpdatedAt)] = updatedAt;

        var storage = _storage.GetOrAdd(indexDescriptor.IndexName, _ => new ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>());
        storage[(keyValue.ToString()!, partitionKey)] = document;

        return Task.CompletedTask;
    }

    protected override Task<ProjectionQueryResult<Dictionary<string, object?>>> QueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default
    ) {
        if (!_storage.TryGetValue(indexDescriptor.IndexName, out var storage))
        {
            return Task.FromResult(new ProjectionQueryResult<Dictionary<string, object?>>
            {
                IndexName = indexDescriptor.IndexName,
                TotalRecordsFound = 0,
                Records = new List<QueryResultDocument<Dictionary<string, object?>>>()
            });
        }

        var result = storage
            .Where(x => string.IsNullOrEmpty(partitionKey) || x.Key.PartitionKey == partitionKey)
            .Select(x => new Dictionary<string, object?>(x.Value))
            .AsEnumerable();

        var expression = projectionQuery.FiltersToExpression<Dictionary<string, object?>>();
        if (expression != null)
        {
            var lambda = expression.Compile();
            result = result.Where(lambda);
        }

        if (!string.IsNullOrWhiteSpace(projectionQuery.SearchText) && projectionQuery.SearchText != "*")
        {
            var searchWords = projectionQuery.SearchText.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var schemaProperties = indexDescriptor.ProjectionDocumentSchema.Properties;

            result = result.Where(x => DocumentMatchesSearchText(x, schemaProperties, searchWords));
        }

        if (projectionQuery.OrderBy.Count > 0)
        {
            IOrderedEnumerable<Dictionary<string, object?>>? ordered = null;

            foreach (var sort in projectionQuery.OrderBy)
            {
                Func<Dictionary<string, object?>, object?> keySelector = d =>
                    ResolveNestedSortValue(d, sort.KeyPath, sort.Filters, indexDescriptor.ProjectionDocumentSchema);

                if (ordered == null)
                {
                    ordered = string.Equals(sort.Order, "desc", StringComparison.OrdinalIgnoreCase)
                        ? result.OrderByDescending(keySelector, NullLastComparer.Instance)
                        : result.OrderBy(keySelector, NullLastComparer.Instance);
                }
                else
                {
                    ordered = string.Equals(sort.Order, "desc", StringComparison.OrdinalIgnoreCase)
                        ? ordered.ThenByDescending(keySelector, NullLastComparer.Instance)
                        : ordered.ThenBy(keySelector, NullLastComparer.Instance);
                }
            }

            result = ordered!;
        }

        var totalCount = result.LongCount();

        result = result.Skip(projectionQuery.Offset);

        if (projectionQuery.Limit.HasValue)
        {
            result = result.Take(projectionQuery.Limit.Value);
        }

        return Task.FromResult(new ProjectionQueryResult<Dictionary<string, object?>>
        {
            IndexName = indexDescriptor.IndexName,
            TotalRecordsFound = totalCount,
            Records = result.Select(
                    x => new QueryResultDocument<Dictionary<string, object?>>
                    {
                        Document = x
                    }
                )
                .ToList()
        });
    }

    protected override Task<long> UpdateByQueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery query,
        string? partitionKey,
        Dictionary<string, object?> propertyUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        if (!_storage.TryGetValue(indexDescriptor.IndexName, out var storage))
        {
            return Task.FromResult(0L);
        }

        var documents = storage
            .Where(x => string.IsNullOrEmpty(partitionKey) || x.Key.PartitionKey == partitionKey)
            .Select(x => x.Value)
            .AsEnumerable();

        var expression = query.FiltersToExpression<Dictionary<string, object?>>();
        if (expression != null)
        {
            documents = documents.Where(expression.Compile());
        }

        var materialized = documents.ToList();

        foreach (var doc in materialized)
        {
            foreach (var (key, value) in propertyUpdates)
            {
                doc[key] = value;
            }
            doc[nameof(ProjectionDocument.UpdatedAt)] = updatedAt;
        }

        return Task.FromResult((long)materialized.Count);
    }

    private class NullLastComparer : IComparer<object?>
    {
        public static readonly NullLastComparer Instance = new();

        public int Compare(object? x, object? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return 1;
            if (y == null) return -1;

            if (x is IComparable comparable)
            {
                return comparable.CompareTo(y);
            }

            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
    }

    private static object? ResolveNestedSortValue(
        Dictionary<string, object?> document,
        string keyPath,
        List<SortingFilter> filters,
        ProjectionDocumentSchema schema)
    {
        var pathParts = keyPath.Split('.');

        if (pathParts.Length == 1)
        {
            return document.TryGetValue(keyPath, out var val) ? val : null;
        }

        var topLevelName = pathParts[0];
        if (!document.TryGetValue(topLevelName, out var topValue) || topValue == null)
        {
            return null;
        }

        var propertySchema = schema.Properties.FirstOrDefault(p => p.PropertyName == topLevelName);
        if (propertySchema == null)
        {
            return null;
        }

        if (propertySchema.IsNestedObject)
        {
            return NavigateDictionary(topValue as Dictionary<string, object?>, pathParts.Skip(1).ToArray());
        }

        if (propertySchema.IsNestedArray)
        {
            if (topValue is not List<object?> list || list.Count == 0)
            {
                return null;
            }

            if (filters.Count > 0)
            {
                foreach (var item in list)
                {
                    if (item is not Dictionary<string, object?> itemDict)
                    {
                        continue;
                    }

                    var matchesAllFilters = true;
                    foreach (var filter in filters)
                    {
                        var filterParts = filter.FilterKeyPath.Split('.');
                        var filterPropName = filterParts.Length > 1 ? filterParts[^1] : filterParts[0];

                        if (!itemDict.TryGetValue(filterPropName, out var filterVal)
                            || !ValuesAreEqual(filterVal, filter.FilterValue))
                        {
                            matchesAllFilters = false;
                            break;
                        }
                    }

                    if (matchesAllFilters)
                    {
                        return NavigateDictionary(itemDict, pathParts.Skip(1).ToArray());
                    }
                }

                return null;
            }

            if (list[0] is Dictionary<string, object?> firstDict)
            {
                return NavigateDictionary(firstDict, pathParts.Skip(1).ToArray());
            }

            return null;
        }

        return null;
    }

    private static object? NavigateDictionary(Dictionary<string, object?>? dict, string[] pathParts)
    {
        if (dict == null) return null;

        object? current = dict;
        foreach (var part in pathParts)
        {
            if (current is not Dictionary<string, object?> currentDict)
            {
                return null;
            }

            if (!currentDict.TryGetValue(part, out current))
            {
                return null;
            }
        }

        return current;
    }

    private static bool ValuesAreEqual(object? a, object? b)
    {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;

        try
        {
            if (a.GetType() != b.GetType())
            {
                var converted = Convert.ChangeType(b, a.GetType());
                return a.Equals(converted);
            }

            return a.Equals(b);
        }
        catch
        {
            return a.ToString() == b.ToString();
        }
    }

    private static bool DocumentMatchesSearchText(
        Dictionary<string, object?> document,
        List<ProjectionDocumentPropertySchema> properties,
        string[] searchWords)
    {
        var searchableValues = new List<string>();
        CollectSearchableValues(document, properties, searchableValues);

        return searchWords.All(word =>
            searchableValues.Any(val => val.Contains(word, StringComparison.OrdinalIgnoreCase))
        );
    }

    private static void CollectSearchableValues(
        Dictionary<string, object?> document,
        List<ProjectionDocumentPropertySchema> properties,
        List<string> result)
    {
        foreach (var prop in properties)
        {
            if (!document.TryGetValue(prop.PropertyName, out var value) || value == null)
            {
                continue;
            }

            if (prop.IsNestedObject && prop.NestedObjectProperties != null)
            {
                if (value is Dictionary<string, object?> nestedDict)
                {
                    CollectSearchableValues(nestedDict, prop.NestedObjectProperties, result);
                }
            }
            else if (prop.IsNestedArray && prop.NestedObjectProperties != null)
            {
                if (value is List<object?> list)
                {
                    foreach (var item in list)
                    {
                        if (item is Dictionary<string, object?> itemDict)
                        {
                            CollectSearchableValues(itemDict, prop.NestedObjectProperties, result);
                        }
                    }
                }
            }
            else if (prop.IsSearchable && value is string s)
            {
                result.Add(s);
            }
        }
    }
}
