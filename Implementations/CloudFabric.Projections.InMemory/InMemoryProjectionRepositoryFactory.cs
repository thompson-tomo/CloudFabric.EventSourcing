using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections.InMemory;

public class InMemoryProjectionRepositoryFactory : ProjectionRepositoryFactory
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<(string Id, string PartitionKey), Dictionary<string, object?>>> _storage = new();

    public InMemoryProjectionRepositoryFactory(ILoggerFactory loggerFactory): base(loggerFactory)
    {
    }

    public override IProjectionRepository<TProjectionDocument> GetProjectionRepository<TProjectionDocument>()
    {
        var cached = GetFromCache<TProjectionDocument>();
        if (cached != null)
        {
            return cached;
        }

        var repository = new InMemoryProjectionRepository<TProjectionDocument>(_storage, _loggerFactory);

        SetToCache<TProjectionDocument>(repository);
        return repository;
    }

    public override ProjectionRepository GetProjectionRepository(ProjectionDocumentSchema projectionDocumentSchema)
    {
        var cached = GetFromCache(projectionDocumentSchema);
        if (cached != null)
        {
            return cached;
        }

        var repository = new InMemoryProjectionRepository(projectionDocumentSchema, _storage, _loggerFactory);

        SetToCache(projectionDocumentSchema, repository);
        return repository;
    }
}
