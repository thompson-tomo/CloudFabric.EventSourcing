namespace CloudFabric.EventSourcing.EventStore;

/// <summary>
/// Represents the progress state of a batch operation (import, update, etc.).
/// Stored in the metadata repository and can be polled by frontend for progress display.
/// </summary>
public record BatchOperationState
{
    public string OperationId { get; init; } = string.Empty;
    public string OperationType { get; init; } = string.Empty;
    public long TotalItems { get; set; }
    public long ProcessedItems { get; set; }
    public string Status { get; set; } = "pending";
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public DateTime LastUpdatedAt { get; set; }
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Optional metadata for storing operation-specific data.
    /// For projection rebuilds: index statuses, schema info, projection name.
    /// </summary>
    public Dictionary<string, object?>? Metadata { get; set; }
}
