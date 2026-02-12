namespace CloudFabric.EventSourcing.EventStore;

/// <summary>
/// Tracks progress of batch operations (import, update, projection rebuild) by persisting state to <see cref="IMetadataRepository"/>.
/// Frontend can poll progress via <see cref="LoadAsync"/>.
/// Supports multi-tenancy via partition key parameter.
/// </summary>
public class BatchOperationTracker
{
    private const string DefaultPartitionKey = "batch-operations";
    private readonly IMetadataRepository _metadataRepository;
    private readonly string _partitionKey;

    public BatchOperationState State { get; private set; }

    private BatchOperationTracker(IMetadataRepository metadataRepository, BatchOperationState state, string partitionKey)
    {
        _metadataRepository = metadataRepository;
        State = state;
        _partitionKey = partitionKey;
    }

    /// <summary>
    /// Starts a new batch operation and persists its initial state.
    /// Returns a tracker instance for reporting progress.
    /// Uses the default partition key.
    /// </summary>
    public static Task<BatchOperationTracker> StartAsync(
        IMetadataRepository metadataRepository,
        string operationType,
        long totalItems,
        CancellationToken cancellationToken = default)
    {
        return StartAsync(metadataRepository, operationType, totalItems, DefaultPartitionKey, cancellationToken);
    }

    /// <summary>
    /// Starts a new batch operation and persists its initial state.
    /// Returns a tracker instance for reporting progress.
    /// </summary>
    /// <param name="metadataRepository">Repository for persisting state.</param>
    /// <param name="operationType">Type of operation (e.g. "import", "update", "projection-rebuild").</param>
    /// <param name="totalItems">Total number of items to process.</param>
    /// <param name="partitionKey">Partition key for multi-tenancy isolation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task<BatchOperationTracker> StartAsync(
        IMetadataRepository metadataRepository,
        string operationType,
        long totalItems,
        string partitionKey,
        CancellationToken cancellationToken = default)
    {
        var state = new BatchOperationState
        {
            OperationId = Guid.NewGuid().ToString(),
            OperationType = operationType,
            TotalItems = totalItems,
            Status = "in_progress",
            StartedAt = DateTime.UtcNow,
            LastUpdatedAt = DateTime.UtcNow
        };
        await metadataRepository.UpsertItem(state.OperationId, partitionKey, state, cancellationToken);
        return new BatchOperationTracker(metadataRepository, state, partitionKey);
    }

    /// <summary>
    /// Updates the number of processed items and persists the state.
    /// Call this after each chunk is processed.
    /// </summary>
    public async Task UpdateProgressAsync(long processedItems, CancellationToken cancellationToken = default)
    {
        State = State with { ProcessedItems = processedItems, LastUpdatedAt = DateTime.UtcNow };
        await _metadataRepository.UpsertItem(State.OperationId, _partitionKey, State, cancellationToken);
    }

    /// <summary>
    /// Updates the number of processed items and metadata, then persists the state.
    /// Use this overload to attach operation-specific data (e.g. index statuses during projection rebuild).
    /// </summary>
    public async Task UpdateProgressAsync(long processedItems, Dictionary<string, object?>? metadata, CancellationToken cancellationToken = default)
    {
        State = State with { ProcessedItems = processedItems, Metadata = metadata, LastUpdatedAt = DateTime.UtcNow };
        await _metadataRepository.UpsertItem(State.OperationId, _partitionKey, State, cancellationToken);
    }

    /// <summary>
    /// Marks the operation as completed.
    /// </summary>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        State = State with
        {
            Status = "completed",
            CompletedAt = DateTime.UtcNow,
            LastUpdatedAt = DateTime.UtcNow
        };
        await _metadataRepository.UpsertItem(State.OperationId, _partitionKey, State, cancellationToken);
    }

    /// <summary>
    /// Marks the operation as failed with an error message.
    /// </summary>
    public async Task FailAsync(string errorMessage, CancellationToken cancellationToken = default)
    {
        State = State with
        {
            Status = "failed",
            ErrorMessage = errorMessage,
            LastUpdatedAt = DateTime.UtcNow
        };
        await _metadataRepository.UpsertItem(State.OperationId, _partitionKey, State, cancellationToken);
    }

    /// <summary>
    /// Loads the progress state for a batch operation by its ID.
    /// Use this for frontend polling. Uses the default partition key.
    /// </summary>
    public static Task<BatchOperationState?> LoadAsync(
        IMetadataRepository metadataRepository,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        return LoadAsync(metadataRepository, operationId, DefaultPartitionKey, cancellationToken);
    }

    /// <summary>
    /// Loads the progress state for a batch operation by its ID and partition key.
    /// Use this for frontend polling with multi-tenancy support.
    /// </summary>
    public static Task<BatchOperationState?> LoadAsync(
        IMetadataRepository metadataRepository,
        string operationId,
        string partitionKey,
        CancellationToken cancellationToken = default)
    {
        return metadataRepository.LoadItem<BatchOperationState>(operationId, partitionKey, cancellationToken);
    }
}
