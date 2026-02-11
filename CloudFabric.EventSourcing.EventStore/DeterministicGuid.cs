using System.Text;

namespace CloudFabric.EventSourcing.EventStore;

public static class DeterministicGuid
{
    /// <summary>
    /// Creates a stable, deterministic Guid from an input string using XxHash128.
    /// Used to derive a global stream_id from the target aggregate type name,
    /// so that cross-aggregate events for Product are in a separate stream from Order.
    /// </summary>
    public static Guid Create(string input)
    {
        var hash = new System.IO.Hashing.XxHash128();
        hash.Append(Encoding.UTF8.GetBytes(input));
        return new Guid(hash.GetCurrentHash());
    }
}
