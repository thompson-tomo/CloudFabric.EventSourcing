using System.Text.Json;

namespace CloudFabric.EventSourcing.EventStore;

public static class EventStoreSerializerOptions
{
    public static readonly JsonSerializerOptions Options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
}
