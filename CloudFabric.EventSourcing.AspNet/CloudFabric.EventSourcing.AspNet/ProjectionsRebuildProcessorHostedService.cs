using CloudFabric.Projections.Worker;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace CloudFabric.EventSourcing.AspNet;

public class ProjectionsRebuildProcessorHostedService : IHostedService, IDisposable
{
    private readonly ProjectionsRebuildProcessor _projectionsRebuildProcessor;
    private readonly IOptions<ProjectionsRebuildProcessorOptions> _options;
    private readonly Action? _onDispose;
    private Task? _runningTask;
    private CancellationTokenSource? _cts;

    public ProjectionsRebuildProcessorHostedService(
        ProjectionsRebuildProcessor projectionsRebuildProcessor,
        IOptions<ProjectionsRebuildProcessorOptions> options,
        Action? onDispose = null
    ) {
        _projectionsRebuildProcessor = projectionsRebuildProcessor;
        _options = options;
        _onDispose = onDispose;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = Task.Run(() => RunAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
        }

        if (_runningTask != null)
        {
            await Task.WhenAny(_runningTask, Task.Delay(Timeout.Infinite, cancellationToken));
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _projectionsRebuildProcessor.RebuildProjectionsThatRequireRebuild(
                    _options.Value.MaxParallelTasks, cancellationToken: cancellationToken
                );

                await Task.Delay(1000, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Graceful shutdown
                break;
            }
            catch (Exception)
            {
                // Avoid tight loop on repeated failures
                await Task.Delay(5000, cancellationToken);
            }
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _onDispose?.Invoke();
    }
}