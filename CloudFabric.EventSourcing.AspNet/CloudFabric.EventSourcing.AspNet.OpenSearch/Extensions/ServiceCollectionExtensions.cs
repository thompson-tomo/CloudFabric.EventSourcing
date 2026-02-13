using CloudFabric.Projections;
using CloudFabric.Projections.OpenSearch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.AspNet.OpenSearch.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddOpenSearchProjections(
            this IEventSourcingBuilder builder,
            OpenSearchBasicAuthConnectionSettings basicAuthConnectionSettings,
            ILoggerFactory loggerFactory,
            bool disableRequestStreaming = false,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddKeyedScoped<ProjectionRepositoryFactory>(
                builder.EventStoreKey,
                (sp, key) => new OpenSearchProjectionRepositoryFactory(
                    basicAuthConnectionSettings,
                    loggerFactory,
                    disableRequestStreaming
                )
            );

            return builder;
        }

        public static IEventSourcingBuilder AddOpenSearchProjections(
            this IEventSourcingBuilder builder,
            OpenSearchAwsAuthConnectionSettings awsAuthConnectionSettings,
            ILoggerFactory loggerFactory,
            bool disableRequestStreaming = false,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddKeyedScoped<ProjectionRepositoryFactory>(
                builder.EventStoreKey,
                (sp, key) => new OpenSearchProjectionRepositoryFactory(
                    awsAuthConnectionSettings,
                    loggerFactory,
                    disableRequestStreaming
                )
            );

            return builder;
        }
    }
}
