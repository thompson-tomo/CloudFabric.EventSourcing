using CloudFabric.Projections;
using Microsoft.Extensions.Logging;

namespace CloudFabric.EventSourcing.EventStore.InMemory;

public class InMemoryEventStoreEventObserver : EventsObserver
{
    private new readonly InMemoryEventStore _eventStore;
    private bool _subscribed;

    public InMemoryEventStoreEventObserver(InMemoryEventStore eventStore, ILogger<InMemoryEventStoreEventObserver> logger): base(eventStore, logger)
    {
        _eventStore = eventStore;
        _eventStore.SubscribeToEventAdded(EventStoreOnEventAdded);
        _subscribed = true;
    }

    public override Task StartAsync(string instanceName)
    {
        if (!_subscribed)
        {
            _eventStore.SubscribeToEventAdded(EventStoreOnEventAdded);
            _subscribed = true;
        }

        return Task.CompletedTask;
    }

    public override Task StopAsync()
    {
        if (_subscribed)
        {
            _eventStore.UnsubscribeFromEventAdded(EventStoreOnEventAdded);
            _subscribed = false;
        }

        return Task.CompletedTask;
    }
}