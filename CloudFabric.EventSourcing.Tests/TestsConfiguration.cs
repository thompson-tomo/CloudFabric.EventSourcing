namespace CloudFabric.EventSourcing.Tests;

/// <summary>
/// Centralized test configuration that reads connection strings from environment variables
/// with fallback to localhost defaults. This allows running tests both locally and in Docker.
/// 
/// Environment variables:
/// - POSTGRES_HOST: PostgreSQL host (default: localhost)
/// - POSTGRES_PORT: PostgreSQL port (default: 5433 - non-standard to avoid conflicts)
/// - POSTGRES_USER: PostgreSQL username (default: cloudfabric_eventsourcing_test)
/// - POSTGRES_PASSWORD: PostgreSQL password (default: cloudfabric_eventsourcing_test)
/// - POSTGRES_DATABASE: PostgreSQL database name (default: cloudfabric_eventsourcing_test)
/// - ELASTICSEARCH_URL: Elasticsearch URL (default: http://localhost:9222 - non-standard to avoid conflicts)
/// - OPENSEARCH_URL: OpenSearch URL (default: http://localhost:9233 - non-standard to avoid conflicts)
/// - COSMOSDB_CONNECTION_STRING: CosmosDB connection string (default: local emulator on port 8089)
/// </summary>
public static class TestsConfiguration
{
    public static string PostgresHost => 
        Environment.GetEnvironmentVariable("POSTGRES_HOST") ?? "localhost";
    
    public static string PostgresPort => 
        Environment.GetEnvironmentVariable("POSTGRES_PORT") ?? "5433";
    
    public static string PostgresUser => 
        Environment.GetEnvironmentVariable("POSTGRES_USER") ?? "cloudfabric_eventsourcing_test";
    
    public static string PostgresPassword => 
        Environment.GetEnvironmentVariable("POSTGRES_PASSWORD") ?? "cloudfabric_eventsourcing_test";
    
    public static string PostgresDatabase => 
        Environment.GetEnvironmentVariable("POSTGRES_DATABASE") ?? "cloudfabric_eventsourcing_test";
    
    public static string PostgresConnectionString =>
        PostgresConnectionStringForDatabase(PostgresDatabase);

    public static string PostgresConnectionStringForDatabase(string database) =>
        $"Host={PostgresHost};Port={PostgresPort};Username={PostgresUser};Password={PostgresPassword};Database={database};Maximum Pool Size=1000;Include Error Detail=true";

    public static string ElasticsearchUrl =>
        Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9222";

    public static string OpenSearchUrl =>
        Environment.GetEnvironmentVariable("OPENSEARCH_URL") ?? "http://localhost:9233";

    /// <summary>
    /// CosmosDB connection string. Default is the local emulator on standard port 8081 with the well-known emulator key.
    /// Use the native Windows CosmosDB Emulator for best performance: https://aka.ms/cosmosdb-emulator
    /// </summary>
    public static string CosmosDbConnectionString => 
        Environment.GetEnvironmentVariable("COSMOSDB_CONNECTION_STRING") 
        ?? "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
}
