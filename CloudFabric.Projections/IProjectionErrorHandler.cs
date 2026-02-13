using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.Projections;

public enum ProjectionErrorBehavior
{
    /// <summary>
    /// Log the error and continue processing other builders and events.
    /// Recommended for production — one failing builder does not stop others.
    /// </summary>
    LogAndContinue,

    /// <summary>
    /// Re-throw the exception, stopping all event processing.
    /// Useful for development/testing where you want fast failure.
    /// </summary>
    StopAll
}

public interface IProjectionErrorHandler
{
    Task OnErrorAsync(
        IProjectionBuilder projectionBuilder,
        IEvent @event,
        Exception exception,
        CancellationToken cancellationToken = default
    );
}
