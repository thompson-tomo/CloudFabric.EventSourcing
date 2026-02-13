using CloudFabric.Projections;
using Microsoft.Extensions.Hosting;

namespace CloudFabric.EventSourcing.AspNet.CosmosDb;

public class CosmosDbProjectionsHostedService : IHostedService, IDisposable
{
    private readonly ProjectionsEngine _projectionsEngine;
    private readonly string _instanceName;
    private readonly Action? _onDispose;

    public CosmosDbProjectionsHostedService(
        ProjectionsEngine projectionsEngine,
        string? instanceName = null,
        Action? onDispose = null)
    {
        _projectionsEngine = projectionsEngine;
        _instanceName = instanceName ?? Environment.MachineName;
        _onDispose = onDispose;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _projectionsEngine.StartAsync(_instanceName);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _projectionsEngine.StopAsync();
    }

    public void Dispose()
    {
        _onDispose?.Invoke();
    }
}
