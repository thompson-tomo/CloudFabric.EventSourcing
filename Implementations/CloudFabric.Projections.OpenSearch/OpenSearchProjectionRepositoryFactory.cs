using CloudFabric.Projections.Resilience;
using Microsoft.Extensions.Logging;

namespace CloudFabric.Projections.OpenSearch;

public class OpenSearchProjectionRepositoryFactory : ProjectionRepositoryFactory
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly OpenSearchBasicAuthConnectionSettings? _basicAuthConnectionSettings;
    private readonly OpenSearchAwsAuthConnectionSettings? _awsAuthConnectionSettings;
    private readonly bool _disableRequestStreaming;
    private readonly ResilienceSettings? _resilienceSettings;

    /// <summary>
    ///
    /// </summary>
    /// <param name="connectionSettings"></param>
    /// <param name="loggerFactory"></param>
    /// <param name="disableRequestStreaming">
    /// When request streaming is disabled, OpenSearch adds debug information about request and response to response object which can
    /// be useful when troubleshooting search problems.
    ///
    /// Defaults to false to improve performance.
    /// </param>
    /// <param name="resilienceSettings">
    /// Optional retry/circuit-breaker settings forwarded to each repository instance.
    /// </param>
    public OpenSearchProjectionRepositoryFactory(
        OpenSearchBasicAuthConnectionSettings connectionSettings,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming = false,
        ResilienceSettings? resilienceSettings = null
    ): base(loggerFactory)
    {
        _basicAuthConnectionSettings = connectionSettings;
        _loggerFactory = loggerFactory;
        _disableRequestStreaming = disableRequestStreaming;
        _resilienceSettings = resilienceSettings;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="awsAuthConnectionSettings"></param>
    /// <param name="loggerFactory"></param>
    /// <param name="disableRequestStreaming">
    /// When request streaming is disabled, OpenSearch adds debug information about request and response to response object which can
    /// be useful when troubleshooting search problems.
    ///
    /// Defaults to false to improve performance.
    /// </param>
    /// <param name="resilienceSettings">
    /// Optional retry/circuit-breaker settings forwarded to each repository instance.
    /// </param>
    public OpenSearchProjectionRepositoryFactory(
        OpenSearchAwsAuthConnectionSettings awsAuthConnectionSettings,
        ILoggerFactory loggerFactory,
        bool disableRequestStreaming = false,
        ResilienceSettings? resilienceSettings = null
    ): base(loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _awsAuthConnectionSettings = awsAuthConnectionSettings;
        _disableRequestStreaming = disableRequestStreaming;
        _resilienceSettings = resilienceSettings;
    }

    public override IProjectionRepository<TProjectionDocument> GetProjectionRepository<TProjectionDocument>()
    {
        var cached = GetFromCache<TProjectionDocument>();
        if (cached != null)
        {
            return cached;
        }

        IProjectionRepository<TProjectionDocument>? repository = null;
        if (_basicAuthConnectionSettings != null)
        {
            repository = new OpenSearchProjectionRepository<TProjectionDocument>(
                _basicAuthConnectionSettings,
                _loggerFactory,
                _disableRequestStreaming,
                _resilienceSettings
            );
        }
        else if (_awsAuthConnectionSettings != null)
        {
            repository = new OpenSearchProjectionRepository<TProjectionDocument>(
                _awsAuthConnectionSettings, _loggerFactory, _disableRequestStreaming,
                _resilienceSettings
            );
        }

        if (repository != null)
        {
            SetToCache(repository);
            return repository;
        }

        throw new Exception("Missed OpenSearch connection settings");
    }

    public override ProjectionRepository GetProjectionRepository(ProjectionDocumentSchema projectionDocumentSchema)
    {
        var cached = GetFromCache(projectionDocumentSchema);
        if (cached != null)
        {
            return cached;
        }

        ProjectionRepository? repository = null;
        if (_basicAuthConnectionSettings != null)
        {
            repository = new OpenSearchProjectionRepository(
                _basicAuthConnectionSettings,
                projectionDocumentSchema,
                _loggerFactory,
                _disableRequestStreaming,
                _resilienceSettings
            );
        }
        else if (_awsAuthConnectionSettings != null)
        {
            repository = new OpenSearchProjectionRepository(
                _awsAuthConnectionSettings,
                projectionDocumentSchema,
                _loggerFactory,
                _disableRequestStreaming,
                _resilienceSettings
            );
        }

        if (repository != null)
        {
            SetToCache(projectionDocumentSchema, repository);
            return repository;
        }
        throw new Exception("Missed OpenSearch connection settings");
    }
}
