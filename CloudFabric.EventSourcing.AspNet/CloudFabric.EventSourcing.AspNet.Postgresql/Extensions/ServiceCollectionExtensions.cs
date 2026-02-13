using CloudFabric.EventSourcing.AspNet.Postgresql.HealthChecks;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using CloudFabric.Projections;
using CloudFabric.Projections.Postgresql;
using CloudFabric.Projections.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudFabric.EventSourcing.AspNet.Postgresql.Extensions
{
    internal class PostgresqlEventSourcingScope
    {
        public required IEventStore EventStore { get; init; }
        public required EventsObserver EventsObserver { get; init; }
        public ProjectionsEngine? ProjectionsEngine { get; init; }
        public required IMetadataRepository MetadataRepository { get; init; }
        public required ISequenceGenerator SequenceGenerator { get; init; }
    }

    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddPostgresqlEventStore(
            this IServiceCollection services,
            string eventsConnectionString,
            string eventsTableName,
            string metadataTableName
        )
        {
            return services.AddPostgresqlEventStore(
                eventsTableName,
                (sp) => new PostgresqlEventStoreStaticConnectionInformationProvider(
                    eventsConnectionString, eventsTableName, metadataTableName
                )
            );
        }

        public static IEventSourcingBuilder AddPostgresqlEventStore(
            this IServiceCollection services,
            string eventStoreKey,
            Func<IServiceProvider, IPostgresqlEventStoreConnectionInformationProvider> connectionInformationProviderFactory
        )
        {
            var builder = new EventSourcingBuilder
            {
                EventStoreKey = eventStoreKey,
                Services = services
            };

            services.AddKeyedScoped<IPostgresqlEventStoreConnectionInformationProvider>(
                eventStoreKey, (provider, o) => connectionInformationProviderFactory(provider)
            );

            services.AddKeyedScoped<PostgresqlEventSourcingScope>(
                eventStoreKey,
                (sp, key) =>
                {
                    var connectionInformationProvider = sp.GetRequiredKeyedService<IPostgresqlEventStoreConnectionInformationProvider>(eventStoreKey);

                    var eventStore = new PostgresqlEventStore(connectionInformationProvider);

                    var eventsObserver = new PostgresqlEventStoreEventObserver(
                        eventStore,
                        sp.GetRequiredService<ILogger<PostgresqlEventStoreEventObserver>>()
                    );

                    ProjectionsEngine? projectionsEngine = null;
                    var projectionsRepositoryFactory = sp.GetKeyedService<ProjectionRepositoryFactory>(eventStoreKey);

                    if (projectionsRepositoryFactory != null && builder.ProjectionBuilderFactories != null)
                    {
                        projectionsEngine = CreateProjectionsEngine(
                            sp, eventsObserver, projectionsRepositoryFactory,
                            builder.ProjectionBuilderFactories, ProjectionOperationIndexSelector.Write
                        );
                    }

                    return new PostgresqlEventSourcingScope
                    {
                        EventStore = eventStore,
                        EventsObserver = eventsObserver,
                        ProjectionsEngine = projectionsEngine,
                        MetadataRepository = new PostgresqlMetadataRepository(connectionInformationProvider),
                        SequenceGenerator = new PostgresqlSequenceGenerator(connectionInformationProvider)
                    };
                }
            );

            services.AddKeyedScoped<IEventStore>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventSourcingScope = sp.GetRequiredKeyedService<PostgresqlEventSourcingScope>(key);

                    return eventSourcingScope.EventStore;
                }
            );

            services.AddKeyedScoped<EventsObserver>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventSourcingScope = sp.GetRequiredKeyedService<PostgresqlEventSourcingScope>(key);

                    return eventSourcingScope.EventsObserver;
                }
            );

            services.AddKeyedScoped<AggregateRepositoryFactory>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventSourcingScope = sp.GetRequiredKeyedService<PostgresqlEventSourcingScope>(key);

                    return new AggregateRepositoryFactory(eventSourcingScope.EventStore);
                }
            );

            services.AddKeyedScoped<IMetadataRepository>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventSourcingScope = sp.GetRequiredKeyedService<PostgresqlEventSourcingScope>(key);

                    return eventSourcingScope.MetadataRepository;
                }
            );

            services.AddKeyedScoped<ISequenceGenerator>(
                eventStoreKey,
                (sp, key) =>
                {
                    var eventSourcingScope = sp.GetRequiredKeyedService<PostgresqlEventSourcingScope>(key);

                    return eventSourcingScope.SequenceGenerator;
                }
            );

            return builder;
        }

        public static IEventSourcingBuilder AddPostgresqlProjections(
            this IEventSourcingBuilder builder,
            string projectionsConnectionString,
            bool includeDebugInformation = false,
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
                    var connectionInformationProvider = sp.GetRequiredKeyedService<IPostgresqlEventStoreConnectionInformationProvider>(key);

                    return new PostgresqlProjectionRepositoryFactory(
                        loggerFactory,
                        connectionInformationProvider.GetConnectionInformation().ConnectionString,
                        connectionInformationProvider.GetConnectionInformation().ConnectionId,
                        includeDebugInformation
                    );
                }
            );

            return builder;
        }

        public static IEventSourcingBuilder AddProjectionsRebuildProcessor(this IEventSourcingBuilder builder)
        {
            var b = (EventSourcingBuilder)builder;
            var eventStoreKey = builder.EventStoreKey;

            builder.Services.AddSingleton<IHostedService>(
                (sp) =>
                {
                    var rebuildProcessorScope = sp.CreateScope();
                    var scopedProvider = rebuildProcessorScope.ServiceProvider;

                    var processor = new ProjectionsRebuildProcessor(
                        scopedProvider.GetRequiredKeyedService<ProjectionRepositoryFactory>(eventStoreKey)
                            .GetProjectionsIndexStateRepository(),
                        connectionId => CreateProjectionsEngineForRebuild(
                            connectionId, scopedProvider, eventStoreKey, b.ProjectionBuilderFactories
                        ),
                        scopedProvider.GetRequiredService<ILogger<ProjectionsRebuildProcessor>>(),
                        metadataRepository: scopedProvider.GetRequiredKeyedService<IMetadataRepository>(eventStoreKey),
                        distributedLock: scopedProvider.GetService<IDistributedLock>()
                    );

                    var options = sp.GetRequiredService<IOptions<ProjectionsRebuildProcessorOptions>>();
                    return new ProjectionsRebuildProcessorHostedService(
                        processor,
                        options,
                        onDispose: () => rebuildProcessorScope.Dispose()
                    );
                }
            );

            return builder;
        }

        private static ProjectionsEngine CreateProjectionsEngine(
            IServiceProvider serviceProvider,
            EventsObserver eventsObserver,
            ProjectionRepositoryFactory repositoryFactory,
            ProjectionBuilderFactory[] projectionBuilderFactories,
            ProjectionOperationIndexSelector indexSelector)
        {
            var errorHandler = serviceProvider.GetService<IProjectionErrorHandler>();
            var engine = new ProjectionsEngine(
                eventsObserver,
                serviceProvider.GetRequiredService<ILogger<ProjectionsEngine>>(),
                errorHandler
            );

            foreach (var factory in projectionBuilderFactories)
            {
                engine.AddProjectionBuilder(factory(serviceProvider, repositoryFactory, indexSelector));
            }

            return engine;
        }

        private static Task<IProjectionsEngine> CreateProjectionsEngineForRebuild(
            string connectionId,
            IServiceProvider serviceProvider,
            string eventStoreKey,
            ProjectionBuilderFactory[]? projectionBuilderFactories)
        {
            var connectionInformationProvider = serviceProvider
                .GetRequiredKeyedService<IPostgresqlEventStoreConnectionInformationProvider>(eventStoreKey);

            var connectionInformation = connectionInformationProvider.GetConnectionInformation(connectionId);
            var eventStore = new PostgresqlEventStore(
                connectionInformation.ConnectionString,
                connectionInformation.TableName,
                connectionInformation.MetadataTableName
            );

            var eventsObserver = new PostgresqlEventStoreEventObserver(
                eventStore,
                serviceProvider.GetRequiredService<ILogger<PostgresqlEventStoreEventObserver>>()
            );

            var repositoryFactory = serviceProvider
                .GetRequiredKeyedService<ProjectionRepositoryFactory>(eventStoreKey);

            ProjectionsEngine engine;
            if (projectionBuilderFactories != null)
            {
                engine = CreateProjectionsEngine(
                    serviceProvider, eventsObserver, repositoryFactory,
                    projectionBuilderFactories, ProjectionOperationIndexSelector.ProjectionRebuild
                );
            }
            else
            {
                engine = new ProjectionsEngine(eventsObserver);
            }

            return Task.FromResult<IProjectionsEngine>(engine);
        }

        /// <summary>
        /// Registers PostgreSQL advisory lock as <see cref="IDistributedLock"/>.
        /// Used by <see cref="ProjectionsRebuildProcessor"/> to prevent concurrent rebuilds across instances.
        /// </summary>
        public static IEventSourcingBuilder UsePostgresqlDistributedLock(
            this IEventSourcingBuilder builder,
            string connectionString)
        {
            builder.Services.AddSingleton<IDistributedLock>(
                new PostgresqlDistributedLock(connectionString));

            return builder;
        }

        /// <summary>
        /// Adds health checks for PostgreSQL event store and projections connectivity.
        /// </summary>
        public static IHealthChecksBuilder AddPostgresqlEventSourcingHealthChecks(
            this IHealthChecksBuilder healthChecksBuilder,
            string eventsConnectionString,
            string eventsTableName,
            string? projectionsConnectionString = null)
        {
            healthChecksBuilder.Add(new HealthCheckRegistration(
                "postgresql-eventstore",
                _ => new PostgresqlEventStoreHealthCheck(eventsConnectionString, eventsTableName),
                failureStatus: HealthStatus.Unhealthy,
                tags: new[] { "ready", "eventstore" }
            ));

            if (projectionsConnectionString != null)
            {
                healthChecksBuilder.Add(new HealthCheckRegistration(
                    "postgresql-projections",
                    _ => new PostgresqlProjectionsHealthCheck(projectionsConnectionString),
                    failureStatus: HealthStatus.Unhealthy,
                    tags: new[] { "ready", "projections" }
                ));
            }

            return healthChecksBuilder;
        }
    }
}
