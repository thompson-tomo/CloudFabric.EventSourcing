using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.Projections;

public interface IProjectionErrorHandler
{
    Task OnError(IProjectionBuilder projectionBuilder, IEvent @event, Exception exception);
}

public class LogAndContinueProjectionErrorHandler : IProjectionErrorHandler
{
    private readonly Action<IProjectionBuilder, IEvent, Exception> _logAction;

    public LogAndContinueProjectionErrorHandler(Action<IProjectionBuilder, IEvent, Exception> logAction)
    {
        _logAction = logAction;
    }

    public Task OnError(IProjectionBuilder projectionBuilder, IEvent @event, Exception exception)
    {
        _logAction(projectionBuilder, @event, exception);
        return Task.CompletedTask;
    }
}
