namespace CloudFabric.Projections;

public enum ObjectTypeHintEnum
{
    Guid = 0
}

public class ProjectionDocumentPropertySchema
{
    public string PropertyName { get; set; }
    public TypeCode PropertyType { get; set; }
    
    /// <summary>
    /// There are types basic to dotnet that don't have their own TypeCode.
    /// Good example is Guid - GetTypeCode returns `Object` for guids, but we can't store and deserialize it using simple object rules with properties.
    /// Hence, for such cases there is an additional ObjectTypeHint value - this way we can control edge cases for serializing/deserializing values;
    /// </summary>
    public ObjectTypeHintEnum? ObjectTypeHint { get; set; } 
    
    public bool IsKey { get; set; } = false;
    public bool IsSearchable { get; set; } = false;
    public bool IsRetrievable { get; set; } = true;
    public string[] SynonymMaps { get; set; } = Array.Empty<string>();
    public double SearchableBoost { get; set; } = 1;
    public bool IsFilterable { get; set; } = false;
    public bool IsSortable { get; set; } = false;
    public bool IsFacetable { get; set; } = false;
    public string? Analyzer { get; set; }
    public string? SearchAnalyzer { get; set; }
    public string? IndexAnalyzer { get; set; }
    public bool UseForSuggestions { get; set; } = false;
    public double[] FacetableRanges { get; set; } = Array.Empty<double>();

    public bool IsNestedObject { get; set; } = false;
    public bool IsNestedArray { get; set; } = false;
    public TypeCode? ArrayElementType { get; set; }
    
    /// <summary>
    /// See ObjectTypeHint property.
    /// </summary>
    public ObjectTypeHintEnum? ArrayElementTypeObjectTypeHint { get; set; }
    
    // nested object/array properties schema
    public List<ProjectionDocumentPropertySchema> NestedObjectProperties { get; set; }


    public override string ToString()
    {
        return $"ProjectionDocumentPropertySchema:{PropertyName}: {PropertyType}";
    }

    public ProjectionDocumentPropertySchema Clone()
    {
        var clone = (ProjectionDocumentPropertySchema)MemberwiseClone();

        if (SynonymMaps != null)
        {
            clone.SynonymMaps = (string[])SynonymMaps.Clone();
        }

        if (FacetableRanges != null)
        {
            clone.FacetableRanges = (double[])FacetableRanges.Clone();
        }

        if (NestedObjectProperties != null)
        {
            clone.NestedObjectProperties = NestedObjectProperties.Select(p => p.Clone()).ToList();
        }

        return clone;
    }
}