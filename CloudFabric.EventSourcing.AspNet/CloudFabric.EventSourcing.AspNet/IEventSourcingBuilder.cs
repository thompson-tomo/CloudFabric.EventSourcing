using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.AspNet;

/// <summary>
/// Factory delegate for creating projection builders.
/// Replaces the old reflection-based ConstructProjectionBuilder approach.
/// </summary>
public delegate IProjectionBuilder ProjectionBuilderFactory(
    IServiceProvider serviceProvider,
    ProjectionRepositoryFactory repositoryFactory,
    ProjectionOperationIndexSelector indexSelector
);

public interface IEventSourcingBuilder
{
    string EventStoreKey { get; }
    IServiceCollection Services { get; }

    Task InitializeEventStore(IServiceProvider serviceProvider);
    Task EnsureProjectionIndexFor<T>(IServiceProvider serviceProvider) where T : ProjectionDocument;
}

public static class EventSourcingBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="IProjectionErrorHandler"/> with the specified behavior.
    /// <see cref="ProjectionErrorBehavior.LogAndContinue"/> is recommended for production.
    /// </summary>
    public static IEventSourcingBuilder UseProjectionErrorBehavior(
        this IEventSourcingBuilder builder,
        ProjectionErrorBehavior behavior)
    {
        builder.Services.AddSingleton<IProjectionErrorHandler>(sp =>
            new DefaultProjectionErrorHandler(
                behavior,
                sp.GetRequiredService<ILogger<DefaultProjectionErrorHandler>>()
            )
        );

        return builder;
    }

    /// <summary>
    /// Bridges all keyed event-sourcing services to non-keyed registrations.
    /// Use this in single-store scenarios so that consumers can resolve services
    /// without specifying the store key.
    /// <para>
    /// WARNING: If called multiple times (from different store builders), the last
    /// registration wins. For multi-store scenarios, resolve keyed services directly.
    /// </para>
    /// </summary>
    public static IEventSourcingBuilder ForwardAsNonKeyed(this IEventSourcingBuilder builder)
    {
        var key = builder.EventStoreKey;

        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<IEventStore>(key));
        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<AggregateRepositoryFactory>(key));
        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<ProjectionRepositoryFactory>(key));
        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<IMetadataRepository>(key));
        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<ISequenceGenerator>(key));
        builder.Services.AddScoped(sp => sp.GetRequiredKeyedService<EventsObserver>(key));

        return builder;
    }

    /// <summary>
    /// Registers a repository type as a scoped service, resolving <see cref="IEventStore"/>
    /// from the keyed registration associated with this builder's <see cref="IEventSourcingBuilder.EventStoreKey"/>.
    /// </summary>
    public static IEventSourcingBuilder AddRepository<TRepo>(this IEventSourcingBuilder builder)
        where TRepo : class
    {
        builder.Services.AddScoped(
            sp =>
            {
                var eventStore = sp.GetRequiredKeyedService<IEventStore>(builder.EventStoreKey);
                return ActivatorUtilities.CreateInstance<TRepo>(sp, new object[] { eventStore });
            }
        );

        return builder;
    }
}
