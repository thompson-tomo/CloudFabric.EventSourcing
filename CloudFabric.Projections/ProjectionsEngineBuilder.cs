using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CloudFabric.Projections;

public class ProjectionsEngineBuilder
{
    private EventsObserver? _eventsObserver;
    private ILogger<ProjectionsEngine>? _logger;
    private IProjectionErrorHandler? _errorHandler;
    private readonly List<IProjectionBuilder> _projectionBuilders = new();

    public ProjectionsEngineBuilder WithEventsObserver(EventsObserver eventsObserver)
    {
        _eventsObserver = eventsObserver;
        return this;
    }

    public ProjectionsEngineBuilder AddProjectionBuilder(IProjectionBuilder projectionBuilder)
    {
        _projectionBuilders.Add(projectionBuilder);
        return this;
    }

    public ProjectionsEngineBuilder WithLogger(ILogger<ProjectionsEngine> logger)
    {
        _logger = logger;
        return this;
    }

    public ProjectionsEngineBuilder WithErrorHandler(IProjectionErrorHandler errorHandler)
    {
        _errorHandler = errorHandler;
        return this;
    }

    public ProjectionsEngine Build()
    {
        if (_eventsObserver == null)
        {
            throw new InvalidOperationException(
                "EventsObserver is required. Call WithEventsObserver() before Build()."
            );
        }

        var engine = new ProjectionsEngine(
            _eventsObserver,
            _logger ?? NullLogger<ProjectionsEngine>.Instance,
            _errorHandler
        );

        foreach (var builder in _projectionBuilders)
        {
            engine.AddProjectionBuilder(builder);
        }

        return engine;
    }
}
