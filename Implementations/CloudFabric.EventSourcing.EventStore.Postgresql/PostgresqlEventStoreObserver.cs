using CloudFabric.Projections;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.EventStore.Postgresql;

public class PostgresqlEventStoreEventObserver : EventsObserver
{
    private new readonly PostgresqlEventStore _eventStore;
    private bool _subscribed;

    public PostgresqlEventStoreEventObserver(
        PostgresqlEventStore eventStore,
        ILogger<PostgresqlEventStoreEventObserver> logger
    ): base(eventStore, logger)
    {
        _eventStore = eventStore;
        _eventStore.SubscribeToEventAdded(EventStoreOnEventAdded);
        _subscribed = true;
    }

    public override Task StartAsync(string instanceName)
    {
        _logger.LogInformation("Starting {InstanceName}", instanceName);

        if (!_subscribed)
        {
            _eventStore.SubscribeToEventAdded(EventStoreOnEventAdded);
            _subscribed = true;
        }

        return Task.CompletedTask;
    }

    public override Task StopAsync()
    {
        _logger.LogInformation("Stopping");

        if (_subscribed)
        {
            _eventStore.UnsubscribeFromEventAdded(EventStoreOnEventAdded);
            _subscribed = false;
        }

        return Task.CompletedTask;
    }
}