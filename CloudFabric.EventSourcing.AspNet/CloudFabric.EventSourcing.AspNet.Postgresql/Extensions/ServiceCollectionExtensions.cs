using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using CloudFabric.Projections;
using CloudFabric.Projections.Postgresql;
using CloudFabric.Projections.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CloudFabric.EventSourcing.AspNet.Postgresql.Extensions
{
    class PostgresqlEventSourcingScope
    {
        public IEventStore EventStore { get; set; }
        public EventsObserver EventsObserver { get; set; }
        public ProjectionsEngine? ProjectionsEngine { get; set; }
        public IMetadataRepository MetadataRepository { get; set; }
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
                    var scope = new PostgresqlEventSourcingScope();

                    var connectionInformationProvider = sp.GetRequiredKeyedService<IPostgresqlEventStoreConnectionInformationProvider>(eventStoreKey);

                    scope.EventStore = new PostgresqlEventStore(connectionInformationProvider);

                    scope.EventsObserver = new PostgresqlEventStoreEventObserver(
                        (PostgresqlEventStore)scope.EventStore,
                        sp.GetRequiredService<ILogger<PostgresqlEventStoreEventObserver>>()
                    );

                    var projectionsRepositoryFactory = sp.GetKeyedService<ProjectionRepositoryFactory>(eventStoreKey);

                    if (projectionsRepositoryFactory != null && builder.ProjectionBuilderFactories != null)
                    {
                        scope.ProjectionsEngine = new ProjectionsEngine(scope.EventsObserver);

                        foreach (var factory in builder.ProjectionBuilderFactories)
                        {
                            var projectionBuilder = factory(
                                projectionsRepositoryFactory,
                                ProjectionOperationIndexSelector.Write
                            );

                            scope.ProjectionsEngine.AddProjectionBuilder(projectionBuilder);
                        }

                        // No explicit StartAsync needed - Postgresql observer subscribes in constructor
                    }

                    scope.MetadataRepository = new PostgresqlMetadataRepository(connectionInformationProvider);

                    return scope;
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

            return builder;
        }

        public static IEventSourcingBuilder AddRepository<TRepo>(this IEventSourcingBuilder builder)
            where TRepo : class
        {
            builder.Services.AddScoped(
                (sp) =>
                {
                    var eventStore = sp.GetRequiredKeyedService<IEventStore>(builder.EventStoreKey);
                    return ActivatorUtilities.CreateInstance<TRepo>(sp, new object[] { eventStore });
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
            b.ProjectionsConnectionString = projectionsConnectionString;
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

            // Register as IHostedService directly so the scope is owned by the hosted service
            // and properly disposed on shutdown via IDisposable.
            builder.Services.AddSingleton<IHostedService>(
                (sp) =>
                {
                    var rebuildProcessorScope = sp.CreateScope();

                    var processor = new ProjectionsRebuildProcessor(
                        rebuildProcessorScope.ServiceProvider.GetRequiredKeyedService<ProjectionRepositoryFactory>(builder.EventStoreKey)
                            .GetProjectionsIndexStateRepository(),
                        async (string connectionId) =>
                        {
                            var connectionInformationProvider = rebuildProcessorScope.ServiceProvider
                                .GetRequiredKeyedService<IPostgresqlEventStoreConnectionInformationProvider>(builder.EventStoreKey);

                            var connectionInformation = connectionInformationProvider.GetConnectionInformation(connectionId);
                            var eventStore = new PostgresqlEventStore(
                                connectionInformation.ConnectionString, connectionInformation.TableName, connectionInformation.MetadataTableName
                            );

                            var eventObserver = new PostgresqlEventStoreEventObserver(
                                (PostgresqlEventStore)eventStore,
                                rebuildProcessorScope.ServiceProvider.GetRequiredService<ILogger<PostgresqlEventStoreEventObserver>>()
                            );

                            var projectionsEngine = new ProjectionsEngine(eventObserver);

                            if (b.ProjectionBuilderFactories != null)
                            {
                                foreach (var factory in b.ProjectionBuilderFactories)
                                {
                                    var projectionBuilder = factory(
                                        rebuildProcessorScope.ServiceProvider.GetRequiredKeyedService<ProjectionRepositoryFactory>(builder.EventStoreKey),
                                        ProjectionOperationIndexSelector.ProjectionRebuild
                                    );

                                    projectionsEngine.AddProjectionBuilder(projectionBuilder);
                                }
                            }

                            return projectionsEngine;
                        },
                        rebuildProcessorScope.ServiceProvider.GetRequiredService<ILogger<ProjectionsRebuildProcessor>>()
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
    }
}
