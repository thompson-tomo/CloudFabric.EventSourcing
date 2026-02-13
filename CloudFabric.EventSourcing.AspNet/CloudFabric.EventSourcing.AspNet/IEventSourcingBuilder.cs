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
}
