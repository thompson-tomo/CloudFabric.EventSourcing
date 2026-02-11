using CloudFabric.EventSourcing.EventStore;
using CloudFabric.Projections;
using Microsoft.Extensions.DependencyInjection;

namespace CloudFabric.EventSourcing.AspNet;

/// <summary>
/// Factory delegate for creating projection builders.
/// Replaces the old reflection-based ConstructProjectionBuilder approach.
/// </summary>
public delegate IProjectionBuilder ProjectionBuilderFactory(
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
