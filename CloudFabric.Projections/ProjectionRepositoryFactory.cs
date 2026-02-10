using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections;

public abstract class ProjectionRepositoryFactory
{
    protected readonly ConcurrentDictionary<string, object> _repositories = new();

    protected readonly ILoggerFactory _loggerFactory;

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
}
