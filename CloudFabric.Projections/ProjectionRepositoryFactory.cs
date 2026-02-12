using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections;

public abstract class ProjectionRepositoryFactory
{
    protected readonly ConcurrentDictionary<string, object> _repositories = new();

    protected readonly ILoggerFactory _loggerFactory;

    private bool _batchModeActive;
    private ProjectionOperationIndexSelector _batchIndexSelector = ProjectionOperationIndexSelector.Write;

    public ProjectionRepositoryFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    protected IProjectionRepository<TProjectionDocument>? GetFromCache<TProjectionDocument>() where TProjectionDocument : ProjectionDocument
    {
        var name = typeof(TProjectionDocument).FullName!;

        if (_repositories.TryGetValue(name, out var cached))
        {
            return (IProjectionRepository<TProjectionDocument>)cached;
        }

        return null;
    }

    protected virtual void SetToCache<TProjectionDocument>(IProjectionRepository<TProjectionDocument> repository) where TProjectionDocument : ProjectionDocument
    {
        var name = typeof(TProjectionDocument).FullName!;

        _repositories[name] = repository;

        // Auto-enable batch on newly cached repos when factory is in batch mode
        if (_batchModeActive)
        {
            repository.BeginBatch(_batchIndexSelector);
        }
    }

    protected virtual ProjectionRepository? GetFromCache(ProjectionDocumentSchema? schema)
    {
        var name = GetSchemaKey(schema);

        if (_repositories.TryGetValue(name, out var cached))
        {
            return (ProjectionRepository)cached;
        }

        return null;
    }

    protected virtual void SetToCache(ProjectionDocumentSchema? schema, ProjectionRepository repository)
    {
        var name = GetSchemaKey(schema);

        _repositories[name] = repository;

        // Auto-enable batch on newly cached repos when factory is in batch mode
        if (_batchModeActive)
        {
            repository.BeginBatch(_batchIndexSelector);
        }
    }

    private static string GetSchemaKey(ProjectionDocumentSchema? schema)
    {
        if (schema == null)
        {
            return "empty-schema";
        }

        return $"{schema.SchemaName}_{ProjectionDocumentSchemaFactory.GetPropertiesUniqueHash(schema.Properties)}";
    }

    public abstract IProjectionRepository<TProjectionDocument> GetProjectionRepository<TProjectionDocument>()
        where TProjectionDocument : ProjectionDocument;

    public abstract ProjectionRepository GetProjectionRepository(ProjectionDocumentSchema projectionDocumentSchema);

    public ProjectionRepository GetProjectionsIndexStateRepository()
    {
        return GetProjectionRepository(ProjectionDocumentSchemaFactory.FromTypeWithAttributes<ProjectionIndexState>());
    }

    /// <summary>
    /// Enables batch mode on all currently cached repositories.
    /// Newly created repositories will also start in batch mode automatically.
    /// </summary>
    /// <param name="indexSelector">
    /// Which index to target when flushing. Use <see cref="ProjectionOperationIndexSelector.ProjectionRebuild"/> during rebuild.
    /// </param>
    public void BeginBatchOnAll(ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write)
    {
        _batchModeActive = true;
        _batchIndexSelector = indexSelector;
        foreach (var repo in _repositories.Values)
        {
            if (repo is IProjectionRepository r) r.BeginBatch(indexSelector);
        }
    }

    /// <summary>
    /// Flushes buffered operations on all repositories that are in batch mode.
    /// </summary>
    public async Task FlushBatchOnAllAsync(CancellationToken cancellationToken = default)
    {
        foreach (var repo in _repositories.Values)
        {
            if (repo is IProjectionRepository r && r.IsBatchMode)
            {
                await r.FlushBatchAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Exits batch mode on all repositories and discards any unflushed data.
    /// </summary>
    public void EndBatchOnAll()
    {
        _batchModeActive = false;
        foreach (var repo in _repositories.Values)
        {
            if (repo is IProjectionRepository r) r.EndBatch();
        }
    }
}
