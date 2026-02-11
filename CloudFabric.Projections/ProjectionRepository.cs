using System.Linq.Expressions;
using System.Text.Json;
using CloudFabric.Projections.Exceptions;
using CloudFabric.Projections.Queries;
using CloudFabric.Projections.Utils;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections;


public enum ProjectionOperationIndexSelector
{
    /// <summary>
    /// The most complete and up-to-date index, if exists, will be used, even if properties hash is different (meaning it's an old schema),
    /// new indices in process of rebuild will be ignored.
    /// If there is no complete index, a new index which is being rebuilt will be used.
    /// </summary>
    ReadOnly,
    /// <summary>
    /// The most complete and up-to-date index, if exists, will be used, even if properties hash is different (meaning it's an old schema),
    /// new indices in process of rebuild will be ignored.
    /// If there is no complete index, an exception will be thrown saying that index is not ready yet.
    /// </summary>
    Write,
    /// <summary>
    /// Only the most recent index with exact same schema properties hash will be used. This type is internal and should only be used
    /// when rebuilding projections.
    /// </summary>
    ProjectionRebuild
}

/// <summary>
/// For any operation with projections, ProjectionOperationIndexSelector needs to be provided.
/// Based on indexSelector, the repository will select proper index and will also return it's schema so that
/// repository implementation could know what properties to operate on.
/// </summary>
public class ProjectionOperationIndexDescriptor
{
    public string IndexName { get; set; }
    public ProjectionDocumentSchema ProjectionDocumentSchema { get; set; }
}

public abstract class ProjectionRepository : IProjectionRepository
{
    protected readonly ProjectionDocumentSchema ProjectionDocumentSchema;
    protected readonly ProjectionDocumentSchema ProjectionIndexStateSchema;
    
    protected readonly ILogger<ProjectionRepository> Logger;
    protected const string PROJECTION_INDEX_STATE_INDEX_NAME = "projection_index_state";

    public ProjectionRepository(ProjectionDocumentSchema projectionDocumentSchema, ILogger<ProjectionRepository> logger)
    {
        ProjectionDocumentSchema = (ProjectionDocumentSchema)projectionDocumentSchema.Clone();
        ProjectionIndexStateSchema = ProjectionDocumentSchemaFactory.FromTypeWithAttributes<ProjectionIndexState>();
        Logger = logger;
    }

    public async Task EnsureIndex(CancellationToken cancellationToken = default)
    {
        Logger.LogInformation("Ensuring index exists for {ProjectionDocumentSchemaName}", ProjectionDocumentSchema.SchemaName);

        // we just need to make sure index exists `readOnly` will do that, otherwise it will throw an error saying that index is not ready yet
        var indexName = await GetIndexDescriptorForOperation(ProjectionOperationIndexSelector.ReadOnly, cancellationToken);

        Logger.LogInformation("Index for {ProjectionDocumentSchemaName}, {IndexName}", ProjectionDocumentSchema.SchemaName, indexName);
    }
    
    protected abstract Task CreateIndex(string indexName, ProjectionDocumentSchema projectionDocumentSchema);

    /// <summary>
    /// Drops a specific index by name. Used by stale index cleanup after projection rebuild completes.
    /// </summary>
    protected abstract Task DropIndex(string indexName, CancellationToken cancellationToken = default);
    
    public abstract Task<Dictionary<string, object?>?> Single(
        Guid id, 
        string partitionKey, 
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    );

    public async Task<ProjectionQueryResult<Dictionary<string, object?>>> Query(
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.ReadOnly
    ) {
        var indexDescriptor = await GetIndexDescriptorForOperation(indexSelector, cancellationToken);
        return await QueryInternal(indexDescriptor, projectionQuery, partitionKey, cancellationToken);
    }

    protected abstract Task<ProjectionQueryResult<Dictionary<string, object?>>> QueryInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        ProjectionQuery projectionQuery,
        string? partitionKey = null,
        CancellationToken cancellationToken = default
    );

    public async Task Upsert(
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    )
    {
        var indexDescriptor = await GetIndexDescriptorForOperation(indexSelector, cancellationToken);
        await UpsertInternal(indexDescriptor, document, partitionKey, updatedAt, cancellationToken);
    }

    protected abstract Task UpsertInternal(
        ProjectionOperationIndexDescriptor indexDescriptor,
        Dictionary<string, object?> document, 
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    );
    
    public abstract Task Delete(
        Guid id, 
        string partitionKey, 
        CancellationToken cancellationToken = default, 
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );
    public abstract Task DeleteAll(
        string? partitionKey = null, 
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );

    protected async Task<IReadOnlyCollection<ProjectionIndexState>> QueryProjectionIndexStates(
        ProjectionQuery projectionQuery, 
        CancellationToken cancellationToken = default
    ) {
        try
        {
            var results = await QueryInternal(
                new ProjectionOperationIndexDescriptor()
                {
                    IndexName = PROJECTION_INDEX_STATE_INDEX_NAME,
                    ProjectionDocumentSchema = ProjectionIndexStateSchema
                },
                projectionQuery,
                PROJECTION_INDEX_STATE_INDEX_NAME,
                cancellationToken
            );

            return results.Records
                .Select(
                    doc =>
                        ProjectionDocumentSerializer.DeserializeFromDictionary<ProjectionIndexState>(doc.Document)
                )
                .ToList()
                .AsReadOnly();
        }
        catch (InvalidProjectionSchemaException ex)
        {
            await HandleProjectionIndexStateIndexNotFound();
            return await QueryProjectionIndexStates(projectionQuery, cancellationToken);
        }
    }

    private async Task HandleProjectionIndexStateIndexNotFound()
    {
        Logger.LogInformation($"Projection index state index not found, creating {PROJECTION_INDEX_STATE_INDEX_NAME} index...");

        try
        {
            await CreateIndex(
                PROJECTION_INDEX_STATE_INDEX_NAME,
                ProjectionIndexStateSchema
            );
            
            Logger.LogInformation($"{PROJECTION_INDEX_STATE_INDEX_NAME} index created");
        }
        catch (Exception ex)
        {
            var exception = new Exception($"Failed to create a table for projection \"{PROJECTION_INDEX_STATE_INDEX_NAME}\"", ex);
            throw exception;
        }
    }

    protected async Task<ProjectionIndexState?> GetProjectionIndexState(
        string schemaName,
        CancellationToken cancellationToken = default
    ) {
        var projectionQuery = new ProjectionQuery()
        {
            Filters = new List<Filter>()
            {
                new Filter(
                    $"{nameof(ProjectionIndexState.ProjectionName)}",
                    FilterOperator.Equal,
                    schemaName
                )
            }
        };

        try
        {
            var results = await QueryInternal(
                new ProjectionOperationIndexDescriptor() {
                    IndexName = PROJECTION_INDEX_STATE_INDEX_NAME,
                    ProjectionDocumentSchema = ProjectionIndexStateSchema 
                },
                projectionQuery,
                PROJECTION_INDEX_STATE_INDEX_NAME,
                cancellationToken
            );

            if (results.Records.Count <= 0)
            {
                return null;
            }

            return ProjectionDocumentSerializer.DeserializeFromDictionary<ProjectionIndexState>(results.Records.First().Document);
        }
        catch (InvalidProjectionSchemaException) // on first run there will be no table with a name `PROJECTION_INDEX_STATE_INDEX_NAME`,
        {                                        // we can safely return null, the system will create the table on SaveProjectionIndexState method.
            return null;
        }
    }
    
    protected async Task<ProjectionIndexState?> GetProjectionIndexState(CancellationToken cancellationToken = default) {
        return await GetProjectionIndexState(ProjectionDocumentSchema.SchemaName, cancellationToken);
    }

    public virtual async Task SaveProjectionIndexState(ProjectionIndexState state)
    {
        try
        {
            await UpsertInternal(
                new ProjectionOperationIndexDescriptor() {
                    IndexName = PROJECTION_INDEX_STATE_INDEX_NAME,
                    ProjectionDocumentSchema = ProjectionIndexStateSchema 
                },
                ProjectionDocumentSerializer.SerializeToDictionary(state),
                PROJECTION_INDEX_STATE_INDEX_NAME,
                state.UpdatedAt
            );
        }
        catch (InvalidProjectionSchemaException)
        {
            await HandleProjectionIndexStateIndexNotFound();
            await SaveProjectionIndexState(state);
        }
    }

    /// <summary>
    /// Internal method for getting index name to work with.
    /// When projection schema changes, there can be two indexes - one for old version of schema which should still receive updates and queries
    /// and a new one which should be populated in the background.
    /// This method checks for available indexes and selects the active one. Once a new index is completed projections rebuild process, this method
    /// will immediately return a new one, so all updates will go to the new index. 
    /// </summary>
    /// <returns></returns>
    protected async Task<ProjectionOperationIndexDescriptor> GetIndexDescriptorForOperation(
        ProjectionOperationIndexSelector indexSelector, 
        CancellationToken cancellationToken = default
    ) {
        var projectionIndexState = await GetProjectionIndexState(cancellationToken);
        
        var projectionVersionPropertiesHash = ProjectionDocumentSchemaFactory.GetPropertiesUniqueHash(ProjectionDocumentSchema.Properties);
        var projectionVersionIndexName = $"{ProjectionDocumentSchema.SchemaName}_{projectionVersionPropertiesHash}"
            .ToLower(); // Elastic throws error saying that index must be lowercase

        if (projectionIndexState != null)
        {
            // First of all - check if index statuses contains an index for this particular schema version
            var indexStatusForThisSchemaVersion = projectionIndexState.IndexesStatuses
                .FirstOrDefault(indexStatus => indexStatus.SchemaHash == projectionVersionPropertiesHash);
            
            if (indexStatusForThisSchemaVersion == null)
            {
                // If it does not, we need to create it so that it will be picked up by projections rebuild processor
                projectionIndexState.IndexesStatuses.Add(new IndexStateForSchemaVersion()
                {
                    CreatedAt = DateTime.UtcNow,
                    SchemaHash = projectionVersionPropertiesHash,
                    Schema = JsonSerializer.Serialize(ProjectionDocumentSchema),
                    IndexName = projectionVersionIndexName,
                    RebuildEventsProcessed = 0,
                    RebuildStartedAt = null,
                    RebuildCompletedAt = null,
                    RebuildHealthCheckAt = DateTime.UtcNow
                });
                await CreateIndex(projectionVersionIndexName, ProjectionDocumentSchema);
                await SaveProjectionIndexState(projectionIndexState);
            }

            if (indexSelector == ProjectionOperationIndexSelector.ProjectionRebuild)
            {
                return new ProjectionOperationIndexDescriptor() {
                    IndexName = projectionVersionIndexName,
                    ProjectionDocumentSchema = ProjectionDocumentSchema
                };
            }

            // At least some projection state exists - find the most recent index with completed projections rebuild
            var lastIndexWithRebuiltProjections = projectionIndexState.IndexesStatuses
                .Where(i => i.RebuildCompletedAt != null).MaxBy(i => i.RebuildCompletedAt);

            if (lastIndexWithRebuiltProjections != null)
            {
                return new ProjectionOperationIndexDescriptor() {
                    IndexName = lastIndexWithRebuiltProjections.IndexName,
                    ProjectionDocumentSchema = JsonSerializer.Deserialize<ProjectionDocumentSchema>(lastIndexWithRebuiltProjections.Schema!)!
                };
            }
            
            // At least some projection state exists but there is no index which was completely rebuilt. 
            // In such situation we could only allow reading from this index, because writing to it may break projections
            // events order consistency - if projections rebuild is still in progress we will write an event which happened now before it's preceding
            // events not yet processed by projections rebuild process.
            if (indexSelector == ProjectionOperationIndexSelector.ReadOnly)
            {
                // if there are multiple indexes, we want one that has already started rebuild process.
                var lastIndexWithRebuildStarted = projectionIndexState.IndexesStatuses
                    .Where(i => i.RebuildStartedAt != null).MaxBy(i => i.RebuildStartedAt);

                if (lastIndexWithRebuildStarted != null)
                {
                    return new ProjectionOperationIndexDescriptor() {
                        IndexName = lastIndexWithRebuildStarted.IndexName,
                        ProjectionDocumentSchema = JsonSerializer.Deserialize<ProjectionDocumentSchema>(lastIndexWithRebuildStarted.Schema!)!
                    };
                }
                
                // If there are multiple indexes but none of them started rebuilding, just return the most recently created one.
                var lastIndex = projectionIndexState.IndexesStatuses
                    .MaxBy(i => i.CreatedAt);

                if (lastIndex != null)
                {
                    return new ProjectionOperationIndexDescriptor() {
                        IndexName = lastIndex.IndexName,
                        ProjectionDocumentSchema = JsonSerializer.Deserialize<ProjectionDocumentSchema>(lastIndex.Schema!)!
                    };
                }
            }

            throw new IndexNotReadyException(projectionIndexState);
        }
        else
        {
            // no index state exists, meaning there is no index at all. 
            // Create an empty index state, index background processor is designed to look for records which 
            // were created but not populated, it will start the process of projections rebuild once it finds this new record.
            
            var newProjectionIndexState = new ProjectionIndexState()
            {
                Id = Guid.NewGuid(),
                ProjectionName = ProjectionDocumentSchema.SchemaName,
                ConnectionId = "",
                IndexesStatuses = new List<IndexStateForSchemaVersion>() {
                    new IndexStateForSchemaVersion()
                    {
                        CreatedAt = DateTime.UtcNow,
                        Schema = JsonSerializer.Serialize(ProjectionDocumentSchema),
                        SchemaHash = projectionVersionPropertiesHash,
                        IndexName = projectionVersionIndexName,
                        RebuildEventsProcessed = 0,
                        RebuildStartedAt = null,
                        RebuildCompletedAt = null,
                        RebuildHealthCheckAt = DateTime.UtcNow
                    }
                }
            };

            await CreateIndex(projectionVersionIndexName, ProjectionDocumentSchema);
            await SaveProjectionIndexState(newProjectionIndexState);

            return new ProjectionOperationIndexDescriptor() {
                IndexName = projectionVersionIndexName,
                ProjectionDocumentSchema = ProjectionDocumentSchema
            };
        }
    }
    
    /// <summary>
    /// Removes stale indices that completed rebuild more than <paramref name="gracePeriod"/> ago,
    /// keeping only the most recent completed index per projection.
    /// </summary>
    public async Task<int> CleanupStaleIndicesAsync(TimeSpan gracePeriod, CancellationToken cancellationToken = default)
    {
        var allStates = await QueryProjectionIndexStates(new Queries.ProjectionQuery(), cancellationToken);
        var droppedCount = 0;

        foreach (var state in allStates)
        {
            var completedIndices = state.IndexesStatuses
                .Where(i => i.RebuildCompletedAt != null)
                .OrderByDescending(i => i.RebuildCompletedAt)
                .ToList();

            if (completedIndices.Count <= 1)
            {
                continue;
            }

            // Keep the most recent completed index, remove old ones past grace period
            var threshold = DateTime.UtcNow - gracePeriod;
            var staleIndices = completedIndices
                .Skip(1) // keep the newest
                .Where(i => i.RebuildCompletedAt < threshold)
                .ToList();

            foreach (var staleIndex in staleIndices)
            {
                try
                {
                    await DropIndex(staleIndex.IndexName, cancellationToken);
                    state.IndexesStatuses.Remove(staleIndex);
                    droppedCount++;
                    Logger.LogInformation(
                        "Dropped stale index {IndexName} for projection {ProjectionName} (completed at {CompletedAt})",
                        staleIndex.IndexName, state.ProjectionName, staleIndex.RebuildCompletedAt
                    );
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Failed to drop stale index {IndexName}", staleIndex.IndexName);
                }
            }

            if (staleIndices.Count > 0)
            {
                await SaveProjectionIndexState(state);
            }
        }

        return droppedCount;
    }

    public async Task<(ProjectionIndexState?, string?)> AcquireAndLockProjectionThatRequiresRebuild()
    {
        // we need to round datetime received from the database because postgresql has less precision than dotnet
        // https://stackoverflow.com/questions/51103606/storing-datetime-in-postgresql-without-loosing-precision
        var rebuildHealthCheckThreshold = DateTime.UtcNow.AddMinutes(-5).RoundToMicroseconds();
        
        // we are looking for two possible index states:
        // 1. RebuildStartedAt = null - index was just created and requires rebuild and
        // 2. RebuildCompletedAt = null && RebuildHealthCheckAt < rebuildHealthCheckThreshold - rebuild has started but not completed yet and there
        //    have been no health checks for more than 5 minutes (see rebuildHealthCheckThreshold above). Note that this means we are taking over
        //    a rebuild not completed by another process. That process could still wake up and continue, so it should check the index before continuing
        //    and ensure no other process took over the rebuild process.
        Expression<Func<ProjectionIndexState, bool>> projectionStatesFilterExpression = (state) => state.IndexesStatuses.Any((e) => 
            e.RebuildStartedAt == null || (e.RebuildCompletedAt == null && e.RebuildHealthCheckAt < rebuildHealthCheckThreshold)
        );
        
        var indexesStatusesFilterPredicate = 
            ((projectionStatesFilterExpression.Body as MethodCallExpression).Arguments[1] as Expression<Func<IndexStateForSchemaVersion, bool>>).Compile();
        
        var projectionQuery = ProjectionQueryExpressionExtensions.Where(projectionStatesFilterExpression);

        var result = await QueryProjectionIndexStates(projectionQuery);

        if (result.Count <= 0)
        {
            return (null, null);
        }

        var projectionIndexState = result.First();
        
        var dateTimeStarted = DateTime.UtcNow.RoundToMicroseconds();

        projectionIndexState.UpdatedAt = dateTimeStarted;

        // !Important: this where condition should be in absolute sync with the condition we send to elasticsearch (at the beginning of this method)
        var index = projectionIndexState.IndexesStatuses
            //.Where(s => s.RebuildStartedAt == null || (s.RebuildCompletedAt == null && s.RebuildHealthCheckAt < rebuildHealthCheckThreshold))
            .Where(indexesStatusesFilterPredicate)
            .OrderBy(s => s.CreatedAt)
            .FirstOrDefault();

        if (index == null)
        {
            throw new Exception("QueryProjectionIndexStates returned incorrect results");
        }

        index.RebuildStartedAt = dateTimeStarted;
        index.RebuildHealthCheckAt = dateTimeStarted;
        index.RebuildCompletedAt = null;

        await SaveProjectionIndexState(projectionIndexState);

        // we want to make sure no other process locked this item, the easiest way is to just check 
        // if saved result has exact same updatedAt timestamp and we were saving within this process 
        var indexStateResponse = await GetProjectionIndexState(
            projectionIndexState.ProjectionName
        );
        
        // we need to round datetime received from the database because postgresql has less precision than dotnet
        // https://stackoverflow.com/questions/51103606/storing-datetime-in-postgresql-without-loosing-precision
        if (indexStateResponse != null && dateTimeStarted != indexStateResponse.UpdatedAt)
        {
            // look like some other process updated the item before us. Just ignore this record then - it will be processed by that process.
            return (null, null);
        }

        return (indexStateResponse, index.IndexName);
    }
}