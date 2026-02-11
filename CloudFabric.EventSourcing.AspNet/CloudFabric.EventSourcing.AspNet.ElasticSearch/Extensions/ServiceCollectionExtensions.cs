using CloudFabric.Projections;
using CloudFabric.Projections.ElasticSearch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.AspNet.ElasticSearch.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IEventSourcingBuilder AddElasticSearchProjections(
            this IEventSourcingBuilder builder,
            ElasticSearchBasicAuthConnectionSettings basicAuthConnectionSettings,
            ILoggerFactory loggerFactory,
            bool disableRequestStreaming = false,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddScoped<ProjectionRepositoryFactory>(
                (sp) => new ElasticSearchProjectionRepositoryFactory(
                    basicAuthConnectionSettings,
                    loggerFactory,
                    disableRequestStreaming
                )
            );

            return builder;
        }

        public static IEventSourcingBuilder AddElasticSearchProjections(
            this IEventSourcingBuilder builder,
            ElasticSearchApiKeyAuthConnectionSettings apiKeyAuthConnectionSettings,
            ILoggerFactory loggerFactory,
            bool disableRequestStreaming = false,
            params ProjectionBuilderFactory[] projectionBuilderFactories
        )
        {
            var b = (EventSourcingBuilder)builder;
            b.ProjectionBuilderFactories = projectionBuilderFactories;

            builder.Services.AddScoped<ProjectionRepositoryFactory>(
                (sp) => new ElasticSearchProjectionRepositoryFactory(
                    apiKeyAuthConnectionSettings,
                    loggerFactory,
                    disableRequestStreaming
                )
            );

            return builder;
        }
    }
}
