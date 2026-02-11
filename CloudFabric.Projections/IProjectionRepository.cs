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