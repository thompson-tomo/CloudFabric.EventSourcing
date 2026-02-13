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
            // CosmosClient is thread-safe and should be shared
            var cosmosClient = new CosmosClient(connectionString, cosmosClientOptions);
            var eventStore = new CosmosDbEventStore(cosmosClient, databaseId, eventsContainerId);

            services.AddScoped<AggregateRepositoryFactory>(_ => new AggregateRepositoryFactory(eventStore));

            var metadataRepository = new CosmosDbMetadataRepository(connectionString, cosmosClientOptions, databaseId, itemsContainerId);
            services.AddScoped<IMetadataRepository>(_ => metadataRepository);

            var sequenceGenerator = new CosmosDbSequenceGenerator(cosmosClient, databaseId, itemsContainerId);
            services.AddScoped<ISequenceGenerator>(_ => sequenceGenerator);

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

            return new EventSourcingBuilder
            {
                EventStore = eventStore,
                Services = services
            };
        }

        public static IEventSourcingBuilder AddCosmosDbEventStore(
            this IServiceCollection services,
            CosmosClient client,
            string databaseId,
            string eventsContainerId
        )
        {
            var eventStore = new CosmosDbEventStore(client, databaseId, eventsContainerId);

            return new EventSourcingBuilder
            {
                EventStore = eventStore,
                Services = services
            };
        }

        public static IEventSourcingBuilder AddRepository<TRepo>(this IEventSourcingBuilder builder)
            where TRepo : class
        {
            var b = (EventSourcingBuilder)builder;

            if (b.EventStore == null)
            {
                throw new ArgumentException("Event store is missing");
            }

            builder.Services.AddSingleton(sp => ActivatorUtilities.CreateInstance<TRepo>(sp, new object[] { b.EventStore }));
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

            builder.Services.AddScoped<ProjectionRepositoryFactory>(_ => projectionsRepositoryFactory);

            // CosmosDb uses change feed which is a global background process,
            // so ProjectionsEngine is singleton (unlike PostgreSQL's per-request pattern).
            builder.Services.AddSingleton<ProjectionsEngine>(sp =>
            {
                var changeFeedObserver = sp.GetRequiredService<CosmosDbEventStoreChangeFeedObserver>();
                var projectionsEngine = new ProjectionsEngine(changeFeedObserver);

                foreach (var factory in projectionBuilderFactories)
                {
                    var projectionBuilder = factory(
                        sp,
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
                return new CosmosDbProjectionsHostedService(engine);
            });

            return builder;
        }
    }
}
