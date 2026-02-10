using System.Reflection;
using System.Text;
using CloudFabric.Projections.Attributes;

namespace CloudFabric.Projections;

public static class ProjectionDocumentSchemaFactory
{
    public static ProjectionDocumentSchema FromTypeWithAttributes<T>()
    {
        var schema = new ProjectionDocumentSchema();
        
        schema.Properties = ProjectionDocumentAttribute.GetAllProjectionProperties<T>()
            .Select(p => GetPropertySchema(p.Key, p.Value.DocumentPropertyAttribute, p.Value.NestedDictionary))
            .ToList();

        schema.SchemaName = typeof(T).Name;
        
        return schema;
    }
    
    public static string GetPropertiesUniqueHash(List<ProjectionDocumentPropertySchema> properties)
    {
        var hash = new System.IO.Hashing.XxHash64();
        
        foreach (var prop in properties)
        {
            hash.Append(Encoding.UTF8.GetBytes(prop.PropertyName));
            hash.Append(Encoding.UTF8.GetBytes(prop.PropertyType.ToString()));

            foreach (var attributeProperty in prop.GetType().GetProperties())
            {
                if (attributeProperty.Name != "TypeId")
                {
                    hash.Append(Encoding.UTF8.GetBytes(attributeProperty.Name));
                    var value = attributeProperty.GetValue(prop);

                    if (value != null)
                    {
                        hash.Append(Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty));
                    }
                }
            }
        }
        
        var hashBytes = hash.GetCurrentHash();

        return $"{Convert.ToHexString(hashBytes)}";
    }

    private static ProjectionDocumentPropertySchema GetPropertySchema(
        PropertyInfo propertyInfo,
        ProjectionDocumentPropertyAttribute documentPropertyAttribute,
        object nestedPropertiesDictionary
    )
    {
        var schema = new ProjectionDocumentPropertySchema()
        {
            PropertyName = propertyInfo.Name,
            PropertyType = ProjectionDocumentAttribute.GetPropertyTypeCode(propertyInfo),
            IsKey = documentPropertyAttribute.IsKey,
            IsSearchable = documentPropertyAttribute.IsSearchable,
            IsRetrievable = documentPropertyAttribute.IsRetrievable,
            SynonymMaps = documentPropertyAttribute.SynonymMaps,
            SearchableBoost = documentPropertyAttribute.SearchableBoost,
            IsFilterable = documentPropertyAttribute.IsFilterable,
            IsSortable = documentPropertyAttribute.IsSortable,
            IsFacetable = documentPropertyAttribute.IsFacetable,
            Analyzer = documentPropertyAttribute.Analyzer,
            SearchAnalyzer = documentPropertyAttribute.SearchAnalyzer,
            IndexAnalyzer = documentPropertyAttribute.IndexAnalyzer,
            UseForSuggestions = documentPropertyAttribute.UseForSuggestions,
            FacetableRanges = documentPropertyAttribute.FacetableRanges,
            IsNestedObject = documentPropertyAttribute.IsNestedObject,
            IsNestedArray = documentPropertyAttribute.IsNestedArray,
            ArrayElementType = documentPropertyAttribute.IsNestedArray
                    ? Type.GetTypeCode(propertyInfo.PropertyType.GenericTypeArguments[0])
                    : null,
            NestedObjectProperties = (documentPropertyAttribute.IsNestedObject || documentPropertyAttribute.IsNestedArray)
                    ? GetNestedObjectProperties(nestedPropertiesDictionary as Dictionary<PropertyInfo, (ProjectionDocumentPropertyAttribute, object)>)
                    : null
        };
        if (schema.PropertyType == TypeCode.Object)
        {
            schema.ObjectTypeHint = GetObjectTypeHintEnum(propertyInfo.PropertyType);
        }

        if (schema.ArrayElementType == TypeCode.Object)
        {
            schema.ArrayElementTypeObjectTypeHint = GetObjectTypeHintEnum(propertyInfo.PropertyType.GenericTypeArguments[0]);
        }

        return schema;
    }

    private static List<ProjectionDocumentPropertySchema> GetNestedObjectProperties(Dictionary<PropertyInfo, (ProjectionDocumentPropertyAttribute DocumentPropertyAttribute, object NestedDictionary)> nestedProperties)
    {
        return nestedProperties
            .Select(property => GetPropertySchema(property.Key, property.Value.DocumentPropertyAttribute, property.Value.NestedDictionary))
            .ToList();
    }

    private static ObjectTypeHintEnum? GetObjectTypeHintEnum(Type propertyType)
    {
        var objectType = propertyType;
            
        if (propertyType.IsGenericType)
        {
            if (propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                objectType = propertyType.GetGenericArguments()[0];
            }
        };

        if (objectType == typeof(Guid))
        {
            return ObjectTypeHintEnum.Guid;
        }

        return null;
    }
}