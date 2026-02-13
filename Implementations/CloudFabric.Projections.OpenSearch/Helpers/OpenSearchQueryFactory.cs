using CloudFabric.Projections.OpenSearch.Extensions;
using CloudFabric.Projections.Queries;
using OpenSearch.Client;

namespace CloudFabric.Projections.OpenSearch.Helpers;

public static class OpenSearchQueryFactory
{
    // Use PhraseSlop number as default value for limiting maximum number of positions allowed between matching tokens for phrases.
    // Transposed terms have a slop of 2
    public static List<QueryContainer> ConstructSearchQuery(ProjectionDocumentSchema projectionDocumentSchema, string searchText)
    {
        // create 1st-layer query
        var queries = new List<QueryContainer>()
        {
            new BoolQuery()
            {
                Filter = new List<QueryContainer>()
                {
                    new QueryStringQuery() {
                        Query = searchText.EscapeElasticUnsupportedCharacters(),
                        Type = TextQueryType.PhrasePrefix,
                        AllowLeadingWildcard = false,
                        DefaultOperator = Operator.And,
                        PhraseSlop = 20
                    }
                }
            }
        };

        // create nested queries
        queries.AddRange(
            ConstructNestedQueries(projectionDocumentSchema, searchText)
        );

        return queries;
    }

    private static List<QueryContainer> ConstructNestedQueries(ProjectionDocumentSchema projectionDocumentSchema, string searchText)
    {
        var queries = new List<QueryContainer>();

        var nestedSearchableProperties = GetSearchableProperties(projectionDocumentSchema.Properties)
                .Where(x => x.IsNestedArray || x.IsNestedObject);

        foreach (var prop in nestedSearchableProperties)
        {
            queries.Add(
                CreateNestedQuery(prop, prop.PropertyName, searchText.EscapeElasticUnsupportedCharacters())
            );
        }

        return queries;
    }

    // retrieves only searchable properties (for NestedObjectProperties lists also) and marks nested properties as IsSearchable
    private static List<ProjectionDocumentPropertySchema> GetSearchableProperties(List<ProjectionDocumentPropertySchema> properties)
    {
        var list = new List<ProjectionDocumentPropertySchema>();

        foreach (var prop in properties)
        {
            if (prop.IsSearchable)
            {
                list.Add(prop);
            }

            if ((prop.IsNestedObject || prop.IsNestedArray) && prop.NestedObjectProperties?.Any() == true)
            {
                var nestedSearchableProperties = GetSearchableProperties(prop.NestedObjectProperties);

                if (nestedSearchableProperties.Any())
                {
                    // we don't need all the fields to create a search query here
                    list.Add(new ProjectionDocumentPropertySchema
                    {
                        PropertyName = prop.PropertyName,
                        IsNestedArray = prop.IsNestedArray,
                        IsNestedObject = prop.IsNestedObject,
                        NestedObjectProperties = nestedSearchableProperties
                    });
                }
            }
        }

        return list;
    }

    // it's supposed that NestedObjectProperties field contains only IsSearchable=true properties
    // it is done via GetSearchableProperties method
    private static QueryContainer CreateNestedQuery(ProjectionDocumentPropertySchema searchableProperty, string nestedPath, string searchText)
    {
        bool containsNotNestedSearchableProperties = searchableProperty.NestedObjectProperties?.Any(x => !x.IsNestedObject && !x.IsNestedArray) == true;
        var nestedSearchableProperties = searchableProperty.NestedObjectProperties?.Where(x => x.IsNestedObject || x.IsNestedArray).ToList() ?? new();

        var nestedQueries = new List<QueryContainer>();
        if (containsNotNestedSearchableProperties)
        {
            nestedQueries.Add(new BoolQuery()
            {
                Filter = new List<QueryContainer>()
                {
                    new QueryStringQuery() {
                        Query = searchText.EscapeElasticUnsupportedCharacters(),
                        Type = TextQueryType.PhrasePrefix,
                        AllowLeadingWildcard = false,
                        DefaultOperator = Operator.And,
                        PhraseSlop = 20
                    }
                },
            });
        }

        if (nestedSearchableProperties.Any())
        {
            nestedQueries.AddRange(
                nestedSearchableProperties.Select(prop => CreateNestedQuery(prop, $"{nestedPath}.{prop.PropertyName}", searchText))
            );
        }

        return new NestedQuery()
        {
            Path = nestedPath,
            Query = new BoolQuery()
            {
                Should = nestedQueries
            }
        };
    }
}
