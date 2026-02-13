using System.Text.Json;
using System.Text.Json.Serialization;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.CosmosDb;

[TestClass]
public class SequenceGeneratorTestsCosmosDb : SequenceGeneratorTests
{
    private const string DatabaseName = "TestDatabase";
    private const string SequenceContainerName = "TestSequenceContainer";

    CosmosClient _cosmosClient = null;
    CosmosClientOptions _cosmosClientOptions;

    private ISequenceGenerator? _sequenceGenerator = null;

    public async Task SetUp()
    {
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

    protected override async Task<ISequenceGenerator> GetSequenceGenerator()
    {
        if (_sequenceGenerator == null)
        {
            await SetUp();

            _sequenceGenerator = new CosmosDbSequenceGenerator(
                _cosmosClient,
                DatabaseName,
                SequenceContainerName
            );
            await _sequenceGenerator.Initialize();
        }

        return _sequenceGenerator;
    }
}
