using System.Text.Json;
using System.Text.Json.Serialization;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.CosmosDb;

[TestClass]
public class MetadataRepositoryTestsCosmosDb : MetadataRepositoryTests
{
    private const string DatabaseName = "TestDatabase";
    private const string ItemContainerName = "TestItemContainer";

    CosmosClient _cosmosClient = null;
    CosmosClientOptions _cosmosClientOptions;

    private IMetadataRepository? _metadataRepository = null;
    private ILogger _logger;

    public async Task SetUp()
    {
        var loggerFactory = new LoggerFactory();
        _logger = loggerFactory.CreateLogger<MetadataRepositoryTestsCosmosDb>();

        JsonSerializerOptions jsonSerializerOptions = new JsonSerializerOptions()
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
        CosmosDbSystemTextJsonSerializer cosmosSystemTextJsonSerializer
            = new CosmosDbSystemTextJsonSerializer(jsonSerializerOptions);

        _cosmosClientOptions = new CosmosClientOptions()
        {
            Serializer = cosmosSystemTextJsonSerializer,
            HttpClientFactory = () =>
            {
                HttpMessageHandler httpMessageHandler = new HttpClientHandler()
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };

                return new HttpClient(httpMessageHandler);
            },
            ConnectionMode = ConnectionMode.Gateway
        };

        _cosmosClient = new CosmosClient(
            TestsConfiguration.CosmosDbConnectionString,
            _cosmosClientOptions
        );

        var database = await ReCreateDatabase(_cosmosClient, DatabaseName);
        await database.CreateContainerIfNotExistsAsync(new ContainerProperties(ItemContainerName, "/partition_key"));

        ContainerResponse itemContainerResponce =
            await _cosmosClient.GetContainer(DatabaseName, ItemContainerName).ReadContainerAsync();
        // Set the indexing mode to consistent
        itemContainerResponce.Resource.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
        // Add an included path
        itemContainerResponce.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/id" });
        // Add an excluded path
        itemContainerResponce.Resource.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/data" });
    }

    private async Task<Database> ReCreateDatabase(CosmosClient cosmosClient, string databaseName)
    {
        await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
        var database = cosmosClient.GetDatabase(databaseName);
        await database.DeleteAsync();
        await cosmosClient.CreateDatabaseIfNotExistsAsync(
            databaseName,
            ThroughputProperties.CreateManualThroughput(400)
        );
        return cosmosClient.GetDatabase(databaseName);
    }

    protected override async Task<IMetadataRepository> GetStore()
    {
        if (_metadataRepository == null)
        {
            await SetUp();

            _metadataRepository = new CosmosDbMetadataRepository(
                TestsConfiguration.CosmosDbConnectionString,
                _cosmosClientOptions,
                DatabaseName,
                ItemContainerName
            );
            await _metadataRepository.Initialize();
        }

        return _metadataRepository;
    }
}