namespace CloudFabric.Projections.Queries;

public class NestedArrayUpdate
{
    /// <summary>
    /// Name of the array property in the projection document (e.g. "CategoryPaths").
    /// </summary>
    public string ArrayPropertyName { get; init; } = "";

    /// <summary>
    /// Filters to select which array elements should be updated.
    /// Only elements matching ALL filters will have updates applied.
    /// </summary>
    public List<Filter> ElementMatchFilters { get; init; } = new();

    /// <summary>
    /// Updates to apply to the matched array elements.
    /// Each update can optionally have a Condition for fine-grained control.
    /// </summary>
    public List<PropertyUpdate> ElementUpdates { get; init; } = new();
}

public class PropertyUpdate
{
    public string PropertyName { get; init; } = "";
    public PropertyUpdateType UpdateType { get; init; }

    /// <summary>
    /// New value for Set operations.
    /// </summary>
    public object? Value { get; init; }

    /// <summary>
    /// Old prefix for ReplacePrefix operations.
    /// </summary>
    public string? OldPrefix { get; init; }

    /// <summary>
    /// New prefix for ReplacePrefix operations.
    /// </summary>
    public string? NewPrefix { get; init; }

    /// <summary>
    /// Optional condition — when set, this update is only applied to elements
    /// that also match this additional filter. Null means apply to all matched elements.
    /// </summary>
    public Filter? Condition { get; init; }
}

public enum PropertyUpdateType
{
    Set,
    ReplacePrefix
}
