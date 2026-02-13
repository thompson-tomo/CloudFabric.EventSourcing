using CloudFabric.EventSourcing.EventStore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CloudFabric.Projections;

public class DefaultProjectionErrorHandler : IProjectionErrorHandler
{
    private readonly ProjectionErrorBehavior _behavior;
    private readonly ILogger _logger;

    public DefaultProjectionErrorHandler(
        ProjectionErrorBehavior behavior,
        ILogger<DefaultProjectionErrorHandler>? logger = null)
    {
        _behavior = behavior;
        _logger = logger ?? NullLogger<DefaultProjectionErrorHandler>.Instance;
    }

    public Task OnErrorAsync(
        IProjectionBuilder projectionBuilder,
        IEvent @event,
        Exception exception,
        CancellationToken cancellationToken = default)
    {
        _logger.LogError(
            exception,
            "Projection builder {BuilderType} failed to handle event {EventType} for aggregate {AggregateId}",
            projectionBuilder.GetType().Name,
            @event.GetType().Name,
            @event.AggregateId
        );

        if (_behavior == ProjectionErrorBehavior.StopAll)
        {
            throw new InvalidOperationException(
                $"Projection builder {projectionBuilder.GetType().Name} error: {exception.Message}",
                exception
            );
        }

        return Task.CompletedTask;
    }
}
