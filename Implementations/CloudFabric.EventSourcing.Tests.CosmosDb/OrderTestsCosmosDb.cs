using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.CosmosDb;
using CloudFabric.Projections;
using CloudFabric.Projections.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.CosmosDb;

[TestClass]
public class OrderTestsCosmosDb : OrderTests
{
    private const string DatabaseName = "TestDatabase";
    private const string ContainerName = "TestContainer";
    private const string ProjectionsContainerName = "TestProjectionsContainer";

    /**
         * Lease database and container are used to synchonize all changefeed readers
         */
    private const string LeaseDatabaseName = "TestDatabaseLease";

    /**
         * Lease database and container are used to synchonize all changefeed readers
         */
    private const string LeaseContainerName = "TestContainerLease";

    private readonly Dictionary<Type, object> _projectionsRepositories = new();

    CosmosClient _cosmosClient = null;
    CosmosClientOptions _cosmosClientOptions;

    private IEventStore? _eventStore = null;
    private CosmosDbEventStoreChangeFeedObserver? _eventStoreEventsObserver = null;
    private ILogger _logger;

    [TestInitialize]
    public async Task SetUp()
    {
        ProjectionsUpdateDelay = TimeSpan.FromSeconds(30);
    }

    private async Task<Database> ReCreateDatabase(CosmosClient cosmosClient, string databaseName)
    {
        await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

        var database = cosmosClient.GetDatabase(databaseName);
        await database.DeleteAsync();
        await cosmosClient.CreateDatabaseIfNotExistsAsync(
            databaseName
        );
        return cosmosClient.GetDatabase(databaseName);
    }

    protected override async Task<IEventStore> GetEventStore()
    {
        if (_eventStore == null)
        {
            var loggerFactory = new LoggerFactory();
            _logger = loggerFactory.CreateLogger<OrderTestsCosmosDb>();

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
            await database.CreateContainerIfNotExistsAsync(new ContainerProperties(ContainerName, "/eventData/partitionKey"));
            await database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(
                    ProjectionsContainerName,
                    "/partitionKey"
                )
            );
            ContainerResponse containerResponse =
                await _cosmosClient.GetContainer(DatabaseName, ContainerName).ReadContainerAsync();
            // Set the indexing mode to consistent
            containerResponse.Resource.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            // Add an included path
            containerResponse.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/id" });
            containerResponse.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/stream/*" });
            // Add an excluded path
            containerResponse.Resource.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/eventData/*" });


            var leaseDatabase = await ReCreateDatabase(_cosmosClient, LeaseDatabaseName);
            await leaseDatabase.CreateContainerIfNotExistsAsync(
                new ContainerProperties(LeaseContainerName, "/partitionKey")
            );

            // add stored procedure
            var migration = new CosmosDbEventStoreMigration(_cosmosClient, DatabaseName, ContainerName);
            await migration.RunAsync();

            _eventStore = new CosmosDbEventStore(
                _cosmosClient,
                DatabaseName,
                ContainerName                
            );
            await _eventStore.Initialize();
        }

        return _eventStore;
    }

    protected override EventsObserver GetEventStoreEventsObserver()
    {
        if (_eventStoreEventsObserver == null)
        {
            _eventStoreEventsObserver = new CosmosDbEventStoreChangeFeedObserver(
                _cosmosClient,
                DatabaseName,
                ContainerName,
                _cosmosClient,
                LeaseDatabaseName,
                LeaseContainerName,
                "",
                NullLogger<CosmosDbEventStoreChangeFeedObserver>.Instance
            );
        }

        return _eventStoreEventsObserver;
    }

    protected override ProjectionRepositoryFactory GetProjectionRepositoryFactory()
    {
        return new CosmosDbProjectionRepositoryFactory(
            new LoggerFactory(),
            TestsConfiguration.CosmosDbConnectionString,
            _cosmosClientOptions,
            DatabaseName,
            ProjectionsContainerName
        );
    }

    public async Task LoadTest()
    {
        var watch = Stopwatch.StartNew();

        var tasks = new List<Task>();

        for (var i = 0; i < 100; i++)
        {
            for (var j = 0; j < 10; j++)
            {
                tasks.Add(TestPlaceOrderAndAddItem());
            }
        }

        await Task.WhenAll(tasks);

        watch.Stop();

        Console.WriteLine($"It took {watch.Elapsed}!");
    }

    [Ignore]
    public override async Task TestProjectionsNestedObjectsFilter()
    {
        return;
    }

    [Ignore]
    public override async Task TestProjectionsNestedObjectsQuery()
    {
        return;
    }

    [Ignore]
    public override async Task TestProjectionsNestedObjectsSorting()
    {
        return;
    }
}