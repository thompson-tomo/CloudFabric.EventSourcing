using System.Collections.Concurrent;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.InMemory;
using CloudFabric.Projections;
using CloudFabric.Projections.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

                    if (projectionsRepositoryFactory != null && builder.ProjectionBuilderFactories != null)
                    {
                        var projectionsEngine = new ProjectionsEngine(eventStoreObserver);

                        foreach (var factory in builder.ProjectionBuilderFactories)
                        {
                            var projectionBuilder = factory(
                                sp,
                                projectionsRepositoryFactory,
                                ProjectionOperationIndexSelector.Write
                            );

                            projectionsEngine.AddProjectionBuilder(projectionBuilder);
                        }

                        // No explicit StartAsync needed - InMemory observer subscribes in constructor
                    }

                    return eventStore;
                }
            );

            services.AddScoped<IMetadataRepository>(sp => new InMemoryMetadataRepository(itemsContainer));
            services.AddScoped<ISequenceGenerator>(sp => new InMemorySequenceGenerator());

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
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddScoped<ProjectionRepositoryFactory>((sp) =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return new InMemoryProjectionRepositoryFactory(loggerFactory);
            });

            return builder;
        }
    }
}
