using CloudFabric.Projections;
using Microsoft.Extensions.Hosting;

namespace CloudFabric.EventSourcing.AspNet.CosmosDb;

public class CosmosDbProjectionsHostedService : IHostedService
{
    private readonly ProjectionsEngine _projectionsEngine;
    private readonly string _instanceName;

    public CosmosDbProjectionsHostedService(ProjectionsEngine projectionsEngine, string? instanceName = null)
    {
        _projectionsEngine = projectionsEngine;
        _instanceName = instanceName ?? Environment.MachineName;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _projectionsEngine.StartAsync(_instanceName);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _projectionsEngine.StopAsync();
    }
}
