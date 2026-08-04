using System.Reflection;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections.Queries;
using Microsoft.CSharp.RuntimeBinder;

namespace CloudFabric.Projections;

public abstract class ProjectionBuilderBase : IProjectionBuilder
{
    protected readonly ProjectionOperationIndexSelector IndexSelector;
    protected readonly ProjectionRepositoryFactory ProjectionRepositoryFactory;
    public HashSet<Type> HandledEventTypes { get; }

    private readonly HashSet<Type> _crossAggregateEventTypes;

    protected ProjectionBuilderBase(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector
    ) {
        if (indexSelector != ProjectionOperationIndexSelector.Write && indexSelector != ProjectionOperationIndexSelector.ProjectionRebuild)
        {
            throw new ArgumentException($"For projection builder the only possible values are {nameof(ProjectionOperationIndexSelector.Write)} " +
                                        $"and {nameof(ProjectionOperationIndexSelector.ProjectionRebuild)}");
        }

        IndexSelector = indexSelector;

        var interfaces = GetType()
            .FindInterfaces(
                new TypeFilter(
                    (type, _) =>
                        type.IsGenericType && typeof(IHandleEvent<>).IsAssignableFrom(type.GetGenericTypeDefinition())
                ),
                null
            );

        HandledEventTypes = new HashSet<Type>(interfaces.Select(x => x.GenericTypeArguments.First()));

        var crossInterfaces = GetType()
            .FindInterfaces(
                new TypeFilter(
                    (type, _) =>
                        type.IsGenericType && typeof(IHandleCrossAggregateEvent<>).IsAssignableFrom(type.GetGenericTypeDefinition())
                ),
                null
            );

        _crossAggregateEventTypes = new HashSet<Type>(crossInterfaces.Select(x => x.GenericTypeArguments.First()));

        foreach (var eventType in _crossAggregateEventTypes)
        {
            HandledEventTypes.Add(eventType);
        }

        ProjectionRepositoryFactory = projectionRepositoryFactory;
    }

    public async Task ApplyEvent(IEvent @event)
    {
        try
        {
            await (this as dynamic).On((dynamic)@event);
        }
        catch (RuntimeBinderException ex)
        {
            throw new InvalidOperationException(
                $"Builder {GetType().Name} does not implement On({@event.GetType().Name}), " +
                $"but declares IHandleEvent<{@event.GetType().Name}>. " +
                $"Add a public Task On({@event.GetType().Name} @event) method.",
                ex
            );
        }
    }

    public async Task ApplyEvents(List<IEvent> events)
    {
        foreach (var e in events)
        {
            await ApplyEvent(e);
        }
    }

    public async Task ApplyCrossAggregateEvent(ICrossAggregateEvent @event)
    {
        try
        {
            await ((dynamic)this).OnBulk((dynamic)@event);
        }
        catch (RuntimeBinderException ex)
        {
            throw new InvalidOperationException(
                $"Builder {GetType().Name} does not implement OnBulk({@event.GetType().Name}), " +
                $"but declares IHandleCrossAggregateEvent<{@event.GetType().Name}>. " +
                $"Add a public Task OnBulk({@event.GetType().Name} @event) method.",
                ex
            );
        }
    }

    public bool HandlesCrossAggregateEvent(Type eventType)
    {
        return _crossAggregateEventTypes.Contains(eventType);
    }
}

public class ProjectionBuilder : ProjectionBuilderBase
{
    protected ProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector
    ) : base(projectionRepositoryFactory, indexSelector)
    {
    }

    protected Task SetDocumentUpdatedAt(ProjectionDocumentSchema projectionDocumentSchema, Guid id, string partitionKey, DateTime updatedAt, Action? documentNotFound = null)
    {
        return UpdateDocument(
            projectionDocumentSchema,
            id,
            partitionKey,
            updatedAt,
            document => Task.CompletedTask,
            documentNotFound
        );
    }

    protected Task UpsertDocument(
        ProjectionDocumentSchema projectionDocumentSchema,
        Dictionary<string, object?> document,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema)
            .Upsert(document, partitionKey, updatedAt, cancellationToken, IndexSelector);
    }

    protected Task UpdateDocument(
        ProjectionDocumentSchema projectionDocumentSchema,
        Guid id,
        string partitionKey,
        DateTime updatedAt,
        Action<Dictionary<string, object?>> callback,
        Action? documentNotFound = null,
        CancellationToken cancellationToken = default
    ) {
        return UpdateDocument(
            projectionDocumentSchema,
            id,
            partitionKey,
            updatedAt,
            document =>
            {
                callback(document);
                return Task.CompletedTask;
            },
            documentNotFound,
            cancellationToken
        );
    }

    protected async Task UpdateDocument(
        ProjectionDocumentSchema projectionDocumentSchema,
        Guid id,
        string partitionKey,
        DateTime updatedAt,
        Func<Dictionary<string, object?>, Task> callback,
        Action? documentNotFound = null,
        CancellationToken cancellationToken = default
    )
    {
        var repository = ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema);

        Dictionary<string, object?>? document = await repository.Single(id, partitionKey, cancellationToken, IndexSelector);

        if (document == null)
        {
            documentNotFound?.Invoke();
        }
        else
        {
            await callback(document);

            await repository.Upsert(document, partitionKey, updatedAt, cancellationToken, IndexSelector);
        }
    }

    protected async Task UpdateDocuments(
        ProjectionDocumentSchema projectionDocumentSchema,
        ProjectionQuery projectionQuery,
        string partitionKey,
        DateTime updatedAt,
        Action<Dictionary<string, object?>> callback,
        CancellationToken cancellationToken = default
    )
    {
        var repository = ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema);

        var documents = await repository.Query(projectionQuery, partitionKey, cancellationToken, IndexSelector);

        var updateTasks = documents.Records.Select(
            document =>
            {
                callback(document.Document!);

                return repository.Upsert(document.Document!, partitionKey, updatedAt, cancellationToken, IndexSelector);
            }
        );

        await Task.WhenAll(updateTasks);
    }

    protected Task DeleteDocument(
        ProjectionDocumentSchema projectionDocumentSchema,
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        var repository = ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema);

        return repository.Delete(id, partitionKey, cancellationToken, IndexSelector);
    }

    protected Task<long> UpdateByQuery(
        ProjectionDocumentSchema projectionDocumentSchema,
        ProjectionQuery query,
        string? partitionKey,
        Dictionary<string, object?> propertyUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema)
            .UpdateByQuery(query, partitionKey, propertyUpdates, updatedAt, cancellationToken, IndexSelector);
    }

    protected Task<long> UpdateNestedArrayByQuery(
        ProjectionDocumentSchema projectionDocumentSchema,
        ProjectionQuery documentQuery,
        string? partitionKey,
        List<NestedArrayUpdate> nestedArrayUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository(projectionDocumentSchema)
            .UpdateNestedArrayByQuery(documentQuery, partitionKey, nestedArrayUpdates, updatedAt, cancellationToken, IndexSelector);
    }
}

public class ProjectionBuilder<TDocument> : ProjectionBuilderBase, IProjectionBuilder<ProjectionDocument>
    where TDocument : ProjectionDocument
{
    protected ProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector
    ) : base(projectionRepositoryFactory, indexSelector)
    {
    }

    protected Task SetDocumentUpdatedAt(Guid id, string partitionKey, DateTime updatedAt, Action? documentNotFound = null)
    {
        return UpdateDocument(
            id,
            partitionKey,
            updatedAt,
            document => Task.CompletedTask,
            documentNotFound
        );
    }

    protected Task UpsertDocument(TDocument document, string partitionKey, DateTime updatedAt, CancellationToken cancellationToken = default)
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository<TDocument>()
            .Upsert(document, partitionKey, updatedAt, cancellationToken, IndexSelector);
    }

    protected Task UpdateDocument(
        Guid id,
        string partitionKey,
        DateTime updatedAt,
        Action<TDocument> callback,
        Action? documentNotFound = null,
        CancellationToken cancellationToken = default
    )
    {
        return UpdateDocument(
            id,
            partitionKey,
            updatedAt,
            document =>
            {
                callback(document);
                return Task.CompletedTask;
            },
            documentNotFound,
            cancellationToken
        );
    }

    private async Task UpdateDocument(
        Guid id,
        string partitionKey,
        DateTime updatedAt,
        Func<TDocument, Task> callback,
        Action? documentNotFound = null,
        CancellationToken cancellationToken = default
    )
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("id should not be null", nameof(id));
        }

        var repository = ProjectionRepositoryFactory
            .GetProjectionRepository<TDocument>();

        TDocument? document = await repository.Single(id, partitionKey, cancellationToken, IndexSelector);

        if (document == null)
        {
            documentNotFound?.Invoke();
        }
        else
        {
            await callback(document);

            await repository.Upsert(document, partitionKey, updatedAt, cancellationToken, IndexSelector);
        }
    }

    protected async Task UpdateDocuments(
        ProjectionQuery projectionQuery,
        string partitionKey,
        DateTime updatedAt,
        Action<TDocument> callback,
        CancellationToken cancellationToken = default
    )
    {
        var repository = ProjectionRepositoryFactory
            .GetProjectionRepository<TDocument>();

        var documents = await repository.Query(projectionQuery, partitionKey, cancellationToken, IndexSelector);

        var updateTasks = documents.Records.Select(
            document =>
            {
                callback(document.Document!);

                return repository.Upsert(document.Document!, partitionKey, updatedAt, cancellationToken, IndexSelector);
            }
        );

        await Task.WhenAll(updateTasks);
    }

    protected Task DeleteDocument(
        Guid id,
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository<TDocument>()
            .Delete(id, partitionKey, cancellationToken, IndexSelector);
    }

    protected Task<long> UpdateByQuery(
        ProjectionQuery query,
        string? partitionKey,
        Dictionary<string, object?> propertyUpdates,
        DateTime updatedAt,
        CancellationToken cancellationToken = default
    )
    {
        return ProjectionRepositoryFactory
            .GetProjectionRepository<TDocument>()
            .UpdateByQuery(query, partitionKey, propertyUpdates, updatedAt, cancellationToken, IndexSelector);
    }
}
