namespace CloudFabric.Projections.OpenSearch;

public record OpenSearchBasicAuthConnectionSettings(string Uri, string Username, string Password, string CertificateThumbprint)
{
    public readonly string Uri = Uri;
    public readonly string Username = Username;
    public readonly string Password = Password;
    public readonly string CertificateThumbprint = CertificateThumbprint;
}
