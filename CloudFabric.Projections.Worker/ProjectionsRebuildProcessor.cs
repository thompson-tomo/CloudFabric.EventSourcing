using CloudFabric.EventSourcing.EventStore;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections.Worker;

public class ProjectionsRebuildProcessor
{
    private readonly ProjectionRepository _projectionRepository;
    private readonly Func<string, Task<IProjectionsEngine>> _projectionsEngineFactory;
    private readonly ProjectionRepositoryFactory? _repositoryFactory;
    private readonly IMetadataRepository? _metadataRepository;

    private readonly ILogger<ProjectionsRebuildProcessor> _logger;

    public ProjectionsRebuildProcessor(
        ProjectionRepository projectionRepository,
        Func<string, Task<IProjectionsEngine>> projectionsEngineFactory,
        ILogger<ProjectionsRebuildProcessor> logger,
        ProjectionRepositoryFactory? repositoryFactory = null,
        IMetadataRepository? metadataRepository = null
    ) {
        _projectionRepository = projectionRepository;
        _projectionsEngineFactory = projectionsEngineFactory;
        _logger = logger;
        _repositoryFactory = repositoryFactory;
        _metadataRepository = metadataRepository;
    }

    public async Task RebuildProjectionsThatRequireRebuild(
        int maxParallelTasks = 4,
        int maxIterations = 100,
        TimeSpan? staleIndexGracePeriod = null,
        CancellationToken cancellationToken = default
    )
    {
        for (var iteration = 0; iteration < maxIterations && !cancellationToken.IsCancellationRequested; iteration++)
        {
            var tasks = new List<Task<bool>>();

            for (var i = 0; i < maxParallelTasks; i++)
            {
                try
                {
                    var (projectionIndexState, indexNameToRebuild) = await _projectionRepository.AcquireAndLockProjectionThatRequiresRebuild();

                    if (projectionIndexState == null || indexNameToRebuild == null)
                    {
                        break;
                    }

                    tasks.Add(RebuildOneProjectionWhichRequiresRebuild(projectionIndexState, indexNameToRebuild, cancellationToken));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to acquire and lock projections that require rebuild");
                }
            }

            if (tasks.Count <= 0)
            {
                // No more projections to rebuild — clean up stale indices if configured
                if (staleIndexGracePeriod.HasValue)
                {
                    await CleanupStaleIndicesAsync(staleIndexGracePeriod.Value, cancellationToken);
                }

                return;
            }

            var results = await Task.WhenAll(tasks);

            // If all tasks failed, stop retrying
            if (results.All(r => !r))
            {
                _logger.LogWarning("All rebuild tasks failed, stopping retry loop");
                return;
            }
        }
    }

    public async Task<int> CleanupStaleIndicesAsync(TimeSpan gracePeriod, CancellationToken cancellationToken = default)
    {
        var droppedCount = await _projectionRepository.CleanupStaleIndicesAsync(gracePeriod, cancellationToken);

        if (droppedCount > 0)
        {
            _logger.LogInformation("Cleaned up {Count} stale projection indices", droppedCount);
        }

        return droppedCount;
    }

    public async Task<bool> RebuildOneProjectionWhichRequiresRebuild(
        ProjectionIndexState projectionIndexState,
        string indexNameToRebuild,
        CancellationToken cancellationToken = default
    ) {
        BatchOperationTracker? tracker = null;

        try
        {
            var connectionId = projectionIndexState.ConnectionId;

            await using var projectionsEngine = await _projectionsEngineFactory(connectionId);

            var eventStoreStatistics = await projectionsEngine.GetEventStoreStatistics();

            var indexToRebuild = projectionIndexState.IndexesStatuses.First(i => i.IndexName == indexNameToRebuild);

            indexToRebuild.TotalEventsToProcess = eventStoreStatistics.TotalEventsCount;

            await _projectionRepository.SaveProjectionIndexState(projectionIndexState);

            // Start tracking via BatchOperationTracker if metadata repository is available
            if (_metadataRepository != null)
            {
                tracker = await BatchOperationTracker.StartAsync(
                    _metadataRepository, "projection-rebuild",
                    eventStoreStatistics.TotalEventsCount, cancellationToken);
                await tracker.UpdateProgressAsync(0,
                    BuildRebuildMetadata(projectionIndexState, indexNameToRebuild),
                    cancellationToken);
            }

            var instanceName = $"{Environment.MachineName}-{Environment.ProcessId}";

            // Enable batch mode with ProjectionRebuild index selector for rebuild
            _repositoryFactory?.BeginBatchOnAll(ProjectionOperationIndexSelector.ProjectionRebuild);
            try
            {
                async Task ChunkProcessedCallback(int eventsProcessed, IEvent lastProcessedEvent)
                {
                    // Flush batch after each chunk of events
                    if (_repositoryFactory != null)
                    {
                        await _repositoryFactory.FlushBatchOnAllAsync(cancellationToken);
                    }

                    indexToRebuild.RebuildEventsProcessed += eventsProcessed;
                    indexToRebuild.LastProcessedEventTimestamp = lastProcessedEvent.Timestamp;
                    indexToRebuild.RebuildHealthCheckAt = DateTime.UtcNow;

                    await _projectionRepository.SaveProjectionIndexState(projectionIndexState);

                    // Update batch operation tracker
                    if (tracker != null)
                    {
                        await tracker.UpdateProgressAsync(
                            indexToRebuild.RebuildEventsProcessed,
                            BuildRebuildMetadata(projectionIndexState, indexNameToRebuild),
                            cancellationToken);
                    }

                    _logger.LogInformation(
                        "Processed {EventsProcessed}/{TotalEventsInEventStore}",
                        indexToRebuild.RebuildEventsProcessed, indexToRebuild.TotalEventsToProcess
                    );
                }

                await projectionsEngine.ReplayEventsAsync(
                    instanceName, null, indexToRebuild.LastProcessedEventTimestamp,
                    250, ChunkProcessedCallback, cancellationToken
                );

                // Final flush for any remaining buffered items
                if (_repositoryFactory != null)
                {
                    await _repositoryFactory.FlushBatchOnAllAsync(cancellationToken);
                }
            }
            finally
            {
                _repositoryFactory?.EndBatchOnAll();
            }

            // Note: any events that arrived during the replay will be picked up by the live
            // event observer once the rebuild completes and the new index becomes active.

            if (!cancellationToken.IsCancellationRequested)
            {
                indexToRebuild.RebuildHealthCheckAt = DateTime.UtcNow;
                indexToRebuild.RebuildCompletedAt = DateTime.UtcNow;

                await _projectionRepository.SaveProjectionIndexState(projectionIndexState);

                if (tracker != null)
                {
                    await tracker.UpdateProgressAsync(
                        indexToRebuild.RebuildEventsProcessed,
                        BuildRebuildMetadata(projectionIndexState, indexNameToRebuild),
                        cancellationToken);
                    await tracker.CompleteAsync(cancellationToken);
                }
            }
        }
        catch(Exception ex)
        {
            _logger.LogError(ex, "Error rebuilding projection {IndexNameToRebuild}", indexNameToRebuild);

            if (tracker != null)
            {
                await tracker.FailAsync(ex.Message, cancellationToken);
            }

            return false;
        }

        return true;
    }

    private static Dictionary<string, object?> BuildRebuildMetadata(
        ProjectionIndexState state, string indexNameBeingRebuilt)
    {
        var indices = state.IndexesStatuses.Select(idx => new Dictionary<string, object?>
        {
            ["indexName"] = idx.IndexName,
            ["schemaHash"] = idx.SchemaHash,
            ["createdAt"] = idx.CreatedAt,
            ["rebuildStartedAt"] = idx.RebuildStartedAt,
            ["rebuildCompletedAt"] = idx.RebuildCompletedAt,
            ["rebuildEventsProcessed"] = idx.RebuildEventsProcessed,
            ["totalEventsToProcess"] = idx.TotalEventsToProcess,
            ["isBeingRebuilt"] = idx.IndexName == indexNameBeingRebuilt
        }).ToList();

        return new Dictionary<string, object?>
        {
            ["projectionName"] = state.ProjectionName,
            ["connectionId"] = state.ConnectionId,
            ["indexNameBeingRebuilt"] = indexNameBeingRebuilt,
            ["indices"] = indices
        };
    }
}
