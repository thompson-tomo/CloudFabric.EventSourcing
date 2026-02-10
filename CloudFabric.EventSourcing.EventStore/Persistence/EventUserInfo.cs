using System.Text.Json.Serialization;

namespace CloudFabric.EventSourcing.EventStore.Persistence;

public record EventUserInfo
{
    public EventUserInfo()
    {
        UserId = Guid.Empty;
    }

    public EventUserInfo(Guid userId)
    {
        UserId = userId;
    }

    [JsonPropertyName("userId")]
    public Guid UserId { get; init; }
}