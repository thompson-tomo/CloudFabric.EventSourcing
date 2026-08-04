using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections;
using Microsoft.Extensions.DependencyInjection;

namespace CloudFabric.EventSourcing.AspNet;

public class EventSourcingBuilder : IEventSourcingBuilder
{
    public string EventStoreKey { get; set; } = "";
    public required IServiceCollection Services { get; set; }

    public ProjectionBuilderFactory[]? ProjectionBuilderFactories { get; set; }

    public async Task InitializeEventStore(IServiceProvider serviceProvider)
    {
        await using var initScope = serviceProvider.CreateAsyncScope();
        var eventStore = initScope.ServiceProvider.GetRequiredKeyedService<IEventStore>(EventStoreKey);
        await eventStore.Initialize();
    }

    public async Task EnsureProjectionIndexFor<T>(IServiceProvider serviceProvider) where T : ProjectionDocument
    {
        await using var initScope = serviceProvider.CreateAsyncScope();
        var projectionsRepositoryFactory = initScope.ServiceProvider.GetRequiredKeyedService<ProjectionRepositoryFactory>(EventStoreKey);
        var projectionRepository = projectionsRepositoryFactory.GetProjectionRepository<T>();
        await projectionRepository.EnsureIndex();
    }
}
