using System.Collections.Concurrent;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore.InMemory;
using CloudFabric.Projections;
using CloudFabric.Projections.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using IMetadataRepository = CloudFabric.EventSourcing.EventStore.IMetadataRepository;

namespace CloudFabric.EventSourcing.AspNet.InMemory.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddInMemoryEventStore(
            this IServiceCollection services,
            ConcurrentDictionary<(Guid, string), List<string>> eventsContainer,
            Dictionary<(string, string), string> itemsContainer
        )
        {
            var builder = new EventSourcingBuilder
            {
                Services = services
            };

            services.AddScoped<IEventStore>(
                (sp) =>
                {
                    var eventStore = new InMemoryEventStore(eventsContainer);

                    // add events observer for projections
                    var eventStoreObserver = new InMemoryEventStoreEventObserver(
                        eventStore, sp.GetRequiredService<ILogger<InMemoryEventStoreEventObserver>>()
                    );

                    var projectionsRepositoryFactory = sp.GetService<ProjectionRepositoryFactory>();

                    // Postgresql's event observer is synchronous - it just handles all calls to npgsql commands, there is no delay
                    // or log processing. That means that all events are happening in request context, possibly on multiple threads,
                    // so having one global projections builder is more complicated than simply creating new projections builder for each request.
                    if (projectionsRepositoryFactory != null)
                    {
                        var projectionsEngine = new ProjectionsEngine();
                        projectionsEngine.SetEventsObserver(eventStoreObserver);

                        foreach (var projectionBuilderType in builder.ProjectionBuilderTypes)
                        {
                            var projectionBuilder = builder.ConstructProjectionBuilder(
                                projectionBuilderType, 
                                projectionsRepositoryFactory, 
                                new AggregateRepositoryFactory(eventStore), 
                                sp, 
                                ProjectionOperationIndexSelector.Write
                            );

                            projectionsEngine.AddProjectionBuilder(projectionBuilder);
                        }

                        projectionsEngine.Start("");
                    }

                    return eventStore;
                }
            );

            services.AddScoped<IMetadataRepository>(sp => new InMemoryMetadataRepository(itemsContainer));

            return builder;
        }

        public static IEventSourcingBuilder AddInMemoryEventStore(this IServiceCollection services)
        {
            return services.AddInMemoryEventStore(
                new ConcurrentDictionary<(Guid, string), List<string>>(),
                new Dictionary<(string, string), string>()
            );
        }

        public static IEventSourcingBuilder AddRepository<TRepo>(this IEventSourcingBuilder builder)
            where TRepo : class
        {
            builder.Services.AddScoped(
                sp =>
                {
                    var eventStore = sp.GetRequiredService<IEventStore>();
                    return ActivatorUtilities.CreateInstance<TRepo>(sp, new object[] { eventStore });
                }
            );

            return builder;
        }

        public static IEventSourcingBuilder AddInMemoryProjections(
            this IEventSourcingBuilder builder,
            params Type[] projectionBuildersTypes
        )
        {
            builder.ProjectionBuilderTypes = projectionBuildersTypes;

            builder.Services.AddScoped<ProjectionRepositoryFactory>((sp) =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new InMemoryProjectionRepositoryFactory(loggerFactory); 
            });

            return builder;
        }
    }
}