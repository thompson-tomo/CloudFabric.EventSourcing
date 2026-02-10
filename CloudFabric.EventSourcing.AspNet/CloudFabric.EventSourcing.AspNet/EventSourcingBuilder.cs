using System.Reflection;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections;
using Microsoft.Extensions.DependencyInjection;

namespace CloudFabric.EventSourcing.AspNet;

public class EventSourcingBuilder : IEventSourcingBuilder
{
    public string EventStoreKey { get; set; }
    
    public IEventStore EventStore { get; set; }

    public IServiceCollection Services { get; set; }

    public ProjectionsEngine? ProjectionsEngine { get; set; }
    public string? ProjectionsConnectionString { get; set; }
    public Type[]? ProjectionBuilderTypes { get; set; }

    public EventsObserver ProjectionEventsObserver { get; set; }
    

    public dynamic ConstructProjectionBuilder(
        Type projectionBuilderType,
        ProjectionRepositoryFactory projectionsRepositoryFactory,
        AggregateRepositoryFactory aggregateRepositoryFactory,
        IServiceProvider serviceProvider,
        ProjectionOperationIndexSelector indexSelector
    )
    {
        dynamic? projectionBuilder = null;

        ConstructorInfo projectionBuilderConstructor = projectionBuilderType.GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length)
            .First();

        var constructorArguments = new List<dynamic>();
        foreach (var arg in projectionBuilderConstructor.GetParameters())
        {
            // parameters such as AggregateRepositoryFactory or ProjectionRepositoryFactory are bound to event sourcing scope 
            // and we can't get them from serviceProvider - that will cause infinite recursion loop, so we provide them separately from current scope
            if (arg.ParameterType == typeof(AggregateRepositoryFactory))
            {
                constructorArguments.Add(aggregateRepositoryFactory);
            }
            else if (arg.ParameterType == typeof(ProjectionRepositoryFactory))
            {
                constructorArguments.Add(projectionsRepositoryFactory);
            }
            else if (arg.ParameterType == typeof(ProjectionOperationIndexSelector))
            {
                constructorArguments.Add(indexSelector);
            }
            else
            {
                constructorArguments.Add(serviceProvider.GetRequiredService(arg.ParameterType));
            }
        }

        // There are two types of projection builders:
        // First one is ProjectionBuilder<ProjectionDocument> and works with strict projection documents represented by class
        // Second one is just ProjectionBuilder - those projections do not have strict schema and work with raw dictionary {key: value} type of documents.
        if (IsTypedProjectionBuilder(projectionBuilderType))
        {
            projectionBuilder = (IProjectionBuilder<ProjectionDocument>?)Activator.CreateInstance(
                projectionBuilderType, constructorArguments.ToArray()
            );
        }
        else
        {
            projectionBuilder = (IProjectionBuilder)Activator.CreateInstance(
                projectionBuilderType, constructorArguments.ToArray()
            );
        }

        if (projectionBuilder == null)
        {
            throw new Exception("Failed to create projection builder instance: Activator.CreateInstance returned null");
        }

        return projectionBuilder;
    }

    private static bool IsTypedProjectionBuilder(Type? type)
    {
        while (type != null)
        {
            if (type.IsGenericType && type.GetGenericArguments().Any(ta => typeof(ProjectionDocument).IsAssignableFrom(ta)))
            {
                return true;
            }

            type = type.BaseType;
        }

        return false;
    }

    public async Task InitializeEventStore(IServiceProvider serviceProvider)
    {
        using var initScope = serviceProvider.CreateScope();
        var eventStore = initScope.ServiceProvider.GetRequiredKeyedService<IEventStore>(EventStoreKey);
        await eventStore.Initialize();
    }

    public async Task EnsureProjectionIndexFor<T>(IServiceProvider serviceProvider) where T : ProjectionDocument
    {
        using var initScope = serviceProvider.CreateScope();
        var projectionsRepositoryFactory = initScope.ServiceProvider.GetRequiredKeyedService<ProjectionRepositoryFactory>(EventStoreKey);
        var userAccountsProjectionRepository = projectionsRepositoryFactory.GetProjectionRepository<T>();
        await userAccountsProjectionRepository.EnsureIndex();
    }
}