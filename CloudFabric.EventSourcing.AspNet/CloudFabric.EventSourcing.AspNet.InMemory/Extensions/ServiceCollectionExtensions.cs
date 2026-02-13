using System.Collections.Concurrent;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using CloudFabric.Projections;
using CloudFabric.Projections.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IMetadataRepository = CloudFabric.EventSourcing.EventStore.IMetadataRepository;

namespace CloudFabric.EventSourcing.AspNet.InMemory.Extensions
{
    internal class InMemoryEventSourcingScope
    {
        public required IEventStore EventStore { get; init; }
        public required EventsObserver EventsObserver { get; init; }
        public ProjectionsEngine? ProjectionsEngine { get; init; }
        public required IMetadataRepository MetadataRepository { get; init; }
        public required ISequenceGenerator SequenceGenerator { get; init; }
    }

    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddInMemoryEventStore(
            this IServiceCollection services,
            string eventStoreKey,
            ConcurrentDictionary<(Guid, string), List<string>> eventsContainer,
            Dictionary<(string, string), string> itemsContainer
        )
        {
            var builder = new EventSourcingBuilder
            {
                EventStoreKey = eventStoreKey,
                Services = services
            };

            services.AddKeyedScoped<InMemoryEventSourcingScope>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventStore = new InMemoryEventStore(eventsContainer);

                    var eventsObserver = new InMemoryEventStoreEventObserver(
                        eventStore, sp.GetRequiredService<ILogger<InMemoryEventStoreEventObserver>>()
                    );

                    ProjectionsEngine? projectionsEngine = null;
                    var projectionsRepositoryFactory = sp.GetKeyedService<ProjectionRepositoryFactory>(eventStoreKey);

                    if (projectionsRepositoryFactory != null && builder.ProjectionBuilderFactories != null)
                    {
                        var errorHandler = sp.GetService<IProjectionErrorHandler>();
                        projectionsEngine = new ProjectionsEngine(
                            eventsObserver,
                            sp.GetRequiredService<ILogger<ProjectionsEngine>>(),
                            errorHandler
                        );

                        foreach (var factory in builder.ProjectionBuilderFactories)
                        {
                            projectionsEngine.AddProjectionBuilder(
                                factory(sp, projectionsRepositoryFactory, ProjectionOperationIndexSelector.Write)
                            );
                        }
                    }

                    return new InMemoryEventSourcingScope
                    {
                        EventStore = eventStore,
                        EventsObserver = eventsObserver,
                        ProjectionsEngine = projectionsEngine,
                        MetadataRepository = new InMemoryMetadataRepository(itemsContainer),
                        SequenceGenerator = new InMemorySequenceGenerator()
                    };
                }
            );

            services.AddKeyedScoped<IEventStore>(
                eventStoreKey,
                (sp, key) => sp.GetRequiredKeyedService<InMemoryEventSourcingScope>(key).EventStore
            );

            services.AddKeyedScoped<EventsObserver>(
                eventStoreKey,
                (sp, key) => sp.GetRequiredKeyedService<InMemoryEventSourcingScope>(key).EventsObserver
            );

            services.AddKeyedScoped<AggregateRepositoryFactory>(
                eventStoreKey,
                (sp, key) => new AggregateRepositoryFactory(
                    sp.GetRequiredKeyedService<InMemoryEventSourcingScope>(key).EventStore
                )
            );

            services.AddKeyedScoped<IMetadataRepository>(
                eventStoreKey,
                (sp, key) => sp.GetRequiredKeyedService<InMemoryEventSourcingScope>(key).MetadataRepository
            );

            services.AddKeyedScoped<ISequenceGenerator>(
                eventStoreKey,
                (sp, key) => sp.GetRequiredKeyedService<InMemoryEventSourcingScope>(key).SequenceGenerator
            );

            return builder;
        }

        public static IEventSourcingBuilder AddInMemoryEventStore(
            this IServiceCollection services,
            string eventStoreKey = "in-memory"
        )
        {
            return services.AddInMemoryEventStore(
                eventStoreKey,
                new ConcurrentDictionary<(Guid, string), List<string>>(),
                new Dictionary<(string, string), string>()
            );
        }

        public static IEventSourcingBuilder AddInMemoryProjections(
            this IEventSourcingBuilder builder,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddKeyedScoped<ProjectionRepositoryFactory>(
                builder.EventStoreKey,
                (sp, key) =>
                {
                    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                    return new InMemoryProjectionRepositoryFactory(loggerFactory);
                }
            );

            return builder;
        }
    }
}
