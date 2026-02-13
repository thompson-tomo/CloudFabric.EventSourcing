namespace CloudFabric.Projections.OpenSearch;

public record OpenSearchAwsAuthConnectionSettings(string ServiceUrl, string Region)
{
    public readonly string ServiceUrl = ServiceUrl;
    public readonly string Region = Region;
}
