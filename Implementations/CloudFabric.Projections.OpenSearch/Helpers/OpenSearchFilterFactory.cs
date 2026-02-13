using CloudFabric.Projections.Exceptions;
using CloudFabric.Projections.Queries;
using OpenSearch.Client;
using Filter = CloudFabric.Projections.Queries.Filter;

namespace CloudFabric.Projections.OpenSearch.Helpers;

public static class OpenSearchFilterFactory
{
    public static List<QueryContainer> ConstructFilters(
        List<Filter> filters,
        ProjectionDocumentSchema? schema = null)
    {
        var result = new List<QueryContainer>();

        if (filters == null || filters.Count == 0)
        {
            return result;
        }

        var nestedQueries = new Dictionary<string, List<QueryContainer>>();

        foreach (var f in filters)
        {
            var propName = f.PropertyName ?? f.Filters.FirstOrDefault()?.Filter.PropertyName;
            var isNested = propName != null && propName.Contains('.');

            var query = ConstructConditionFilter(f, schema);
            if (query == null) continue;

            if (isNested)
            {
                var pathParts = propName!.Split('.');
                var nestedPath = string.Join(".", pathParts.Take(pathParts.Length - 1));

                if (!nestedQueries.ContainsKey(nestedPath))
                {
                    nestedQueries[nestedPath] = new List<QueryContainer>();
                }

                nestedQueries[nestedPath].Add(query);
            }
            else
            {
                result.Add(query);
            }
        }

        foreach (var entry in nestedQueries)
        {
            result.Add(new NestedQuery
            {
                Path = entry.Key,
                Query = new BoolQuery
                {
                    Filter = entry.Value
                }
            });
        }

        return result;
    }

    private static QueryContainer? ConstructConditionFilter(Filter filter, ProjectionDocumentSchema? schema)
    {
        var thisQuery = ConstructOneConditionFilter(filter, schema);

        if (filter.Filters.Count == 0)
        {
            return thisQuery;
        }

        QueryContainer? current = thisQuery;

        foreach (var connector in filter.Filters)
        {
            var childQuery = ConstructConditionFilter(connector.Filter, schema);
            if (childQuery == null) continue;

            if (current == null)
            {
                current = childQuery;
                continue;
            }

            current = connector.Logic switch
            {
                FilterLogic.And => new BoolQuery
                {
                    Must = new List<QueryContainer> { current, childQuery }
                },
                FilterLogic.Or => new BoolQuery
                {
                    Should = new List<QueryContainer> { current, childQuery },
                    MinimumShouldMatch = 1
                },
                _ => current
            };
        }

        return current;
    }

    private static QueryContainer? ConstructOneConditionFilter(Filter filter, ProjectionDocumentSchema? schema)
    {
        if (string.IsNullOrEmpty(filter.PropertyName) || filter.PropertyName == "*")
        {
            return null;
        }

        if (schema != null)
        {
            var rootPropertyName = filter.PropertyName.Split('.')[0];
            var propSchema = schema.Properties.FirstOrDefault(p => p.PropertyName == rootPropertyName);
            if (propSchema == null)
            {
                throw new ProjectionQueryFilterException(filter.PropertyName, schema.SchemaName);
            }
        }

        var propertyName = filter.PropertyName;

        // Handle null values before any type-specific logic
        if (filter.Value == null)
        {
            return filter.Operator switch
            {
                FilterOperator.Equal => new BoolQuery
                {
                    MustNot = new List<QueryContainer> { new ExistsQuery { Field = propertyName } }
                },
                FilterOperator.NotEqual => new ExistsQuery { Field = propertyName },
                _ => throw new ArgumentException(
                    "Comparing to null should only be via equal or not equal operators.")
            };
        }

        // Handle DateTime
        if (filter.Value is DateTime dateValue)
        {
            return ConstructDateTimeFilter(propertyName, filter.Operator, dateValue);
        }

        // Determine field name for case-insensitive operations
        var isIgnoreCase = filter.Operator
            is FilterOperator.ContainsIgnoreCase
            or FilterOperator.StartsWithIgnoreCase
            or FilterOperator.EndsWithIgnoreCase;

        if (isIgnoreCase)
        {
            propertyName += ".case-insensitive";
        }

        var stringValue = filter.Value.ToString() ?? "";

        if (isIgnoreCase)
        {
            stringValue = stringValue.ToLowerInvariant();
        }

        // Guid fields are mapped as Text in ES (TypeCode.Object → text with standard analyzer).
        // TermQuery doesn't analyze input, so it can't match tokenized Guid values.
        // Use MatchQuery which applies the field's analyzer to the input — works for both keyword and text fields.
        var equalQuery = filter.Value is Guid
            ? (QueryContainer)new MatchQuery { Field = propertyName, Query = stringValue, Operator = Operator.And }
            : new TermQuery { Field = propertyName, Value = filter.Value };

        return filter.Operator switch
        {
            FilterOperator.Equal => equalQuery,
            FilterOperator.NotEqual => new BoolQuery
            {
                MustNot = new List<QueryContainer> { equalQuery }
            },
            FilterOperator.Greater => ConstructNumericRangeQuery(propertyName, filter.Value, greaterThan: true, inclusive: false),
            FilterOperator.GreaterOrEqual => ConstructNumericRangeQuery(propertyName, filter.Value, greaterThan: true, inclusive: true),
            FilterOperator.Lower => ConstructNumericRangeQuery(propertyName, filter.Value, greaterThan: false, inclusive: false),
            FilterOperator.LowerOrEqual => ConstructNumericRangeQuery(propertyName, filter.Value, greaterThan: false, inclusive: true),
            FilterOperator.StartsWith => new WildcardQuery { Field = propertyName, Value = $"{EscapeWildcardValue(stringValue)}*" },
            FilterOperator.StartsWithIgnoreCase => new WildcardQuery { Field = propertyName, Value = $"{EscapeWildcardValue(stringValue)}*" },
            FilterOperator.EndsWith => new WildcardQuery { Field = propertyName, Value = $"*{EscapeWildcardValue(stringValue)}" },
            FilterOperator.EndsWithIgnoreCase => new WildcardQuery { Field = propertyName, Value = $"*{EscapeWildcardValue(stringValue)}" },
            FilterOperator.Contains => new WildcardQuery { Field = propertyName, Value = $"*{EscapeWildcardValue(stringValue)}*" },
            FilterOperator.ContainsIgnoreCase => new WildcardQuery { Field = propertyName, Value = $"*{EscapeWildcardValue(stringValue)}*" },
            FilterOperator.ArrayContains => new TermQuery { Field = propertyName, Value = filter.Value },
            _ => throw new ArgumentException($"Unsupported filter operator: {filter.Operator}")
        };
    }

    private static QueryContainer ConstructDateTimeFilter(
        string propertyName, string filterOperator, DateTime dateValue)
    {
        return filterOperator switch
        {
            FilterOperator.Equal => new DateRangeQuery
            {
                Field = propertyName,
                GreaterThanOrEqualTo = dateValue,
                LessThanOrEqualTo = dateValue
            },
            FilterOperator.NotEqual => new BoolQuery
            {
                MustNot = new List<QueryContainer>
                {
                    new DateRangeQuery
                    {
                        Field = propertyName,
                        GreaterThanOrEqualTo = dateValue,
                        LessThanOrEqualTo = dateValue
                    }
                }
            },
            FilterOperator.Greater => new DateRangeQuery
            {
                Field = propertyName,
                GreaterThan = dateValue
            },
            FilterOperator.GreaterOrEqual => new DateRangeQuery
            {
                Field = propertyName,
                GreaterThanOrEqualTo = dateValue
            },
            FilterOperator.Lower => new DateRangeQuery
            {
                Field = propertyName,
                LessThan = dateValue
            },
            FilterOperator.LowerOrEqual => new DateRangeQuery
            {
                Field = propertyName,
                LessThanOrEqualTo = dateValue
            },
            _ => throw new ArgumentException(
                $"Unsupported filter operator for DateTime: {filterOperator}")
        };
    }

    private static QueryContainer ConstructNumericRangeQuery(
        string propertyName, object value, bool greaterThan, bool inclusive)
    {
        var doubleValue = Convert.ToDouble(value);
        var query = new NumericRangeQuery { Field = propertyName };

        if (greaterThan)
        {
            if (inclusive) query.GreaterThanOrEqualTo = doubleValue;
            else query.GreaterThan = doubleValue;
        }
        else
        {
            if (inclusive) query.LessThanOrEqualTo = doubleValue;
            else query.LessThan = doubleValue;
        }

        return query;
    }

    private static string EscapeWildcardValue(string value)
    {
        return value
            .Replace("\\", "\\\\")
            .Replace("*", "\\*")
            .Replace("?", "\\?");
    }
}
