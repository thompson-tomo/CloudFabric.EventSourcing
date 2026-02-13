using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.CosmosDb;
using CloudFabric.Projections;
using CloudFabric.Projections.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.AspNet.CosmosDb.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddCosmosDbEventStore(
            this IServiceCollection services,
            string eventStoreKey,
            string connectionString,
            CosmosClientOptions cosmosClientOptions,
            string databaseId,
            string eventsContainerId,
            string itemsContainerId,
            CosmosClient leaseClient,
            string leaseDatabaseId,
            string leaseContainerId,
            string processorName
        )
        {
            var builder = new EventSourcingBuilder
            {
                EventStoreKey = eventStoreKey,
                Services = services
            };

            // CosmosClient is thread-safe and should be shared
            var cosmosClient = new CosmosClient(connectionString, cosmosClientOptions);
            var eventStore = new CosmosDbEventStore(cosmosClient, databaseId, eventsContainerId);

            services.AddKeyedSingleton<IEventStore>(eventStoreKey, (_, _) => eventStore);
            services.AddKeyedScoped<AggregateRepositoryFactory>(eventStoreKey, (_, _) => new AggregateRepositoryFactory(eventStore));

            var metadataRepository = new CosmosDbMetadataRepository(connectionString, cosmosClientOptions, databaseId, itemsContainerId);
            services.AddKeyedSingleton<IMetadataRepository>(eventStoreKey, (_, _) => metadataRepository);

            var sequenceGenerator = new CosmosDbSequenceGenerator(cosmosClient, databaseId, itemsContainerId);
            services.AddKeyedSingleton<ISequenceGenerator>(eventStoreKey, (_, _) => sequenceGenerator);

            // Register change feed observer as singleton (background process)
            services.AddSingleton<CosmosDbEventStoreChangeFeedObserver>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<CosmosDbEventStoreChangeFeedObserver>>();
                return new CosmosDbEventStoreChangeFeedObserver(
                    cosmosClient,
                    databaseId,
                    eventsContainerId,
                    leaseClient,
                    leaseDatabaseId,
                    leaseContainerId,
                    processorName,
                    logger
                );
            });

            // Register the change feed observer as keyed EventsObserver too
            services.AddKeyedSingleton<EventsObserver>(
                eventStoreKey,
                (sp, _) => sp.GetRequiredService<CosmosDbEventStoreChangeFeedObserver>()
            );

            return builder;
        }

        public static IEventSourcingBuilder AddCosmosDbEventStore(
            this IServiceCollection services,
            string eventStoreKey,
            CosmosClient client,
            string databaseId,
            string eventsContainerId
        )
        {
            var eventStore = new CosmosDbEventStore(client, databaseId, eventsContainerId);

            var builder = new EventSourcingBuilder
            {
                EventStoreKey = eventStoreKey,
                Services = services
            };

            services.AddKeyedSingleton<IEventStore>(eventStoreKey, (_, _) => eventStore);
            services.AddKeyedScoped<AggregateRepositoryFactory>(eventStoreKey, (_, _) => new AggregateRepositoryFactory(eventStore));

            return builder;
        }

        // NOTE: projection repositories can't work with different databases for now
        public static IEventSourcingBuilder AddCosmosDbProjections(
            this IEventSourcingBuilder builder,
            CosmosProjectionRepositoryConnectionInfo projectionsConnectionInfo,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            var projectionsRepositoryFactory = new CosmosDbProjectionRepositoryFactory(
                projectionsConnectionInfo.LoggerFactory,
                projectionsConnectionInfo.ConnectionString,
                projectionsConnectionInfo.CosmosClientOptions,
                projectionsConnectionInfo.DatabaseId,
                projectionsConnectionInfo.ContainerId
            );

            builder.Services.AddKeyedScoped<ProjectionRepositoryFactory>(
                builder.EventStoreKey, (_, _) => projectionsRepositoryFactory
            );

            // CosmosDb uses change feed which is a global background process,
            // so ProjectionsEngine is singleton (unlike PostgreSQL's per-request pattern).
            // A dedicated scope is created so that projection builder factories can resolve
            // scoped services (e.g. AggregateRepositoryFactory). The scope lives as long as
            // the singleton engine and is disposed when the hosted service shuts down.
            IServiceScope? projectionsScope = null;

            builder.Services.AddSingleton<ProjectionsEngine>(sp =>
            {
                projectionsScope = sp.CreateScope();
                var scopedProvider = projectionsScope.ServiceProvider;

                var changeFeedObserver = sp.GetRequiredService<CosmosDbEventStoreChangeFeedObserver>();
                var errorHandler = sp.GetService<IProjectionErrorHandler>();
                var projectionsEngine = new ProjectionsEngine(
                    changeFeedObserver,
                    sp.GetRequiredService<ILogger<ProjectionsEngine>>(),
                    errorHandler
                );

                foreach (var factory in projectionBuilderFactories)
                {
                    var projectionBuilder = factory(
                        scopedProvider,
                        projectionsRepositoryFactory,
                        ProjectionOperationIndexSelector.Write
                    );

                    projectionsEngine.AddProjectionBuilder(projectionBuilder);
                }

                return projectionsEngine;
            });

            // Hosted service starts/stops the change feed observer via ProjectionsEngine
            builder.Services.AddSingleton<IHostedService>(sp =>
            {
                var engine = sp.GetRequiredService<ProjectionsEngine>();
                return new CosmosDbProjectionsHostedService(engine, onDispose: () => projectionsScope?.Dispose());
            });

            return builder;
        }
    }
}
