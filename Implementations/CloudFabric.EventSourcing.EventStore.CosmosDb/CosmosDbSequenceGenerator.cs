using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;

namespace CloudFabric.EventSourcing.EventStore.CosmosDb;

public class CosmosDbSequenceGenerator : ISequenceGenerator
{
    private readonly CosmosClient _client;
    private readonly string _databaseId;
    private readonly string _containerId;

    private const string StoredProcedureId = "spGetNextValue";

    public CosmosDbSequenceGenerator(
        string connectionString,
        CosmosClientOptions cosmosClientOptions,
        string databaseId,
        string containerId
    )
    {
        _client = new CosmosClient(connectionString, cosmosClientOptions);
        _databaseId = databaseId;
        _containerId = containerId;
    }

    public CosmosDbSequenceGenerator(
        CosmosClient client,
        string databaseId,
        string containerId
    )
    {
        _client = client;
        _databaseId = databaseId;
        _containerId = containerId;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        var database = _client.GetDatabase(_databaseId);
        await database.CreateContainerIfNotExistsAsync(
            new ContainerProperties(_containerId, "/partition_key"),
            cancellationToken: cancellationToken
        );

        await DeployStoredProcedureAsync(cancellationToken);
    }

    public async Task DeleteAll(CancellationToken cancellationToken = default)
    {
        var container = _client.GetContainer(_databaseId, _containerId);

        try
        {
            await container.DeleteContainerAsync(cancellationToken: cancellationToken);
        }
        catch (CosmosException ex)
        {
            if (ex.StatusCode != System.Net.HttpStatusCode.NotFound)
            {
                throw;
            }
        }
    }

    public async Task<long> GetNextValue(
        string sequenceName,
        string partitionKey,
        long increment = 1,
        long startingNumber = 1,
        CancellationToken cancellationToken = default
    )
    {
        var container = _client.GetContainer(_databaseId, _containerId);
        var cosmosPartitionKey = new PartitionKey(partitionKey);

        dynamic[] parameters = { sequenceName, partitionKey, increment, startingNumber };

        var response = await container.Scripts.ExecuteStoredProcedureAsync<long>(
            StoredProcedureId,
            cosmosPartitionKey,
            parameters,
            cancellationToken: cancellationToken
        );

        return response.Resource;
    }

    private async Task DeployStoredProcedureAsync(CancellationToken cancellationToken = default)
    {
        var container = _client.GetContainer(_databaseId, _containerId);

        try
        {
            await container.Scripts.DeleteStoredProcedureAsync(
                StoredProcedureId,
                cancellationToken: cancellationToken
            );
        }
        catch (CosmosException)
        {
            // Ignore - procedure doesn't exist yet
        }

        await container.Scripts.CreateStoredProcedureAsync(
            new StoredProcedureProperties
            {
                Id = StoredProcedureId,
                Body = @"
            function getNextValue(sequenceName, partitionKey, increment, startingNumber) {

                var query =
                    {
                        'query' : 'SELECT * FROM c WHERE c.id = @id',
                        'parameters' : [{ 'name': '@id', 'value': sequenceName }]
                    };

                const isAccepted = __.queryDocuments(__.getSelfLink(), query,
                    function(err, items) {
                        if (err) throw new Error('Query failed: ' + err.message);

                        if (!items || items.length === 0) {
                            var newDoc = {
                                id: sequenceName,
                                partition_key: partitionKey,
                                current_value: startingNumber
                            };

                            var createAccepted = __.createDocument(__.getSelfLink(), newDoc,
                                function(createErr) {
                                    if (createErr) throw new Error('Create failed: ' + createErr.message);
                                    __.response.setBody(startingNumber);
                                });

                            if (!createAccepted) throw new Error('Create not accepted');
                        } else {
                            var doc = items[0];
                            doc.current_value += increment;

                            var replaceAccepted = __.replaceDocument(doc._self, doc,
                                function(replaceErr) {
                                    if (replaceErr) throw new Error('Replace failed: ' + replaceErr.message);
                                    __.response.setBody(doc.current_value);
                                });

                            if (!replaceAccepted) throw new Error('Replace not accepted');
                        }
                    });

                if (!isAccepted) throw new Error('The query was not accepted by the server.');
            }"
            },
            cancellationToken: cancellationToken
        );
    }
}
