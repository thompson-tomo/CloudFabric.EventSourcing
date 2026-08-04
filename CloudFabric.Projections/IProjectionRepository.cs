using CloudFabric.Projections.Queries;

namespace CloudFabric.Projections;

public interface IProjectionRepository
{
    Task EnsureIndex(CancellationToken cancellationToken = default);

    Task<Dictionary<string, object?>?> Single(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    );

    Task<ProjectionQueryResult<Dictionary<string, object?>>> Query(
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    );

    Task Upsert(
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    Task Delete(
        Guid id, 
        string partitionKey, 
        CancellationToken cancellationToken = default, 
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    Task DeleteAll(
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    /// <summary>
    /// Performs a bulk update on all documents matching the query. This is a server-side operation —
    /// documents are NOT loaded into memory. Each backend uses its native bulk update mechanism:
    /// PostgreSQL: UPDATE ... SET ... WHERE ..., ElasticSearch: _update_by_query, InMemory: iterate and update.
    /// </summary>
    /// <param name="query">Filter conditions to select documents for update.</param>
    /// <param name="partitionKey">Partition key to scope the update. Null means all partitions.</param>
    /// <param name="propertyUpdates">Dictionary of property name to new value mappings.</param>
    /// <param name="updatedAt">Timestamp to set on updated documents.</param>
    /// <returns>Number of documents updated.</returns>
    Task<long> UpdateByQuery(
        ProjectionQuery query,
        string? partitionKey,
        Dictionary<string, object?> propertyUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    /// <summary>
    /// Performs a bulk update on nested array elements within documents matching the query.
    /// For each matching document, iterates array elements, applies element-level filters,
    /// and executes property updates (Set or ReplacePrefix) on matched elements.
    /// Each backend uses its native mechanism: PostgreSQL: jsonb_agg + CASE, ElasticSearch: Painless script, InMemory: iterate and update.
    /// </summary>
    /// <param name="documentQuery">Filter conditions to select documents for update.</param>
    /// <param name="partitionKey">Partition key to scope the update. Null means all partitions.</param>
    /// <param name="nestedArrayUpdates">List of array update specifications.</param>
    /// <param name="updatedAt">Timestamp to set on updated documents.</param>
    /// <returns>Number of documents updated.</returns>
    Task<long> UpdateNestedArrayByQuery(
        ProjectionQuery documentQuery,
        string? partitionKey,
        List<NestedArrayUpdate> nestedArrayUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    /// <summary>
    /// Enters batch mode. Subsequent Upsert/Delete calls will be buffered instead of writing immediately.
    /// Call <see cref="FlushBatchAsync"/> to write all buffered operations as a single bulk operation.
    /// Thread-safe: can be used concurrently with live event processing.
    /// </summary>
    /// <param name="indexSelector">
    /// Which index to target when flushing. Defaults to <see cref="ProjectionOperationIndexSelector.Write"/>.
    /// Use <see cref="ProjectionOperationIndexSelector.ProjectionRebuild"/> during rebuild.
    /// </param>
    void BeginBatch(ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write);

    /// <summary>
    /// Writes all buffered documents to the backing store using a backend-specific bulk operation
    /// (PostgreSQL: multi-row INSERT ON CONFLICT, ElasticSearch: _bulk API), then clears the buffer.
    /// No-op if not in batch mode or buffer is empty.
    /// </summary>
    Task FlushBatchAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Exits batch mode and discards any unflushed documents.
    /// </summary>
    void EndBatch();

    /// <summary>
    /// Returns true if the repository is currently in batch mode.
    /// </summary>
    bool IsBatchMode { get; }

    /// <summary>
    /// Controls auto-flush behavior for batch mode. When <see cref="BatchBufferOptions.MaxBufferSize"/> is reached,
    /// the buffer is automatically flushed. Set MaxBufferSize to 0 to disable auto-flush.
    /// </summary>
    BatchBufferOptions BatchBufferOptions { get; set; }
}

public interface IProjectionRepository<TDocument> : IProjectionRepository
    where TDocument : ProjectionDocument
{
    new Task<TDocument?> Single(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    );

    new Task<ProjectionQueryResult<TDocument>> Query(
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    );

    Task Upsert(
        TDocument document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );
}