namespace CloudFabric.Projections.Queries;

public static class FilterQueryStringExtensions
{
    public static Dictionary<string, Filter> FacetFilterShortcuts = new Dictionary<string, Filter>()
    {
    };

    public static Dictionary<string, string> FacetValuesShortcuts = new Dictionary<string, string>()
    {
    };

    public static string DesanitizeValue(string value)
    {
        return System.Net.WebUtility.UrlDecode(value)
            .Replace(";dot;", ".")
            .Replace(";amp;", "&")
            .Replace(";excl;", "!")
            .Replace(";dollar;", "$")
            .Replace(";aps;", "'");
    }

    public static string SanitizeValue(string value)
    {
        return value.Replace(".", ";dot;")
            .Replace("&", ";amp;")
            .Replace("!", ";excl;")
            .Replace("$", ";dollar;")
            .Replace("'", ";aps;");
    }

    public static string Serialize(this Filter filter, bool useShortcuts = true)
    {
        if (useShortcuts && filter.Filters.Count == 0)
        {
            var shortcut = FacetFilterShortcuts.FirstOrDefault(f => f.Value.Tag == filter.Tag);
            if (shortcut.Value != null)
            {
                return shortcut.Key;
            }
        }

        var valueSerialized = "";

        if (filter.Value != null)
        {
            valueSerialized = filter.Value is DateTime dt
                ? dt.ToString("O", System.Globalization.CultureInfo.InvariantCulture)
                : filter.Value.ToString();

            if (FacetValuesShortcuts.ContainsKey(valueSerialized))
            {
                valueSerialized = FacetValuesShortcuts[valueSerialized];
            }

            // replace special characters used for serialization
            valueSerialized = SanitizeValue(valueSerialized);

            if (filter.Value is string)
            {
                valueSerialized = $"'{valueSerialized}'";
            }
        }

        var filtersSerialized = "";

        if (filter.Filters != null && filter.Filters.Count > 0)
        {
            filtersSerialized = string.Join(
                ProjectionQueryQueryStringExtensions.FILTER_NESTED_FILTERS_JOIN_CHARACTER, 
                filter.Filters.Select(f => f.Serialize())
            );
        }

        const char PROPS_JOIN = ProjectionQueryQueryStringExtensions.FILTER_PROPERTIES_JOIN_CHARACTER;

        return $"{(string.IsNullOrEmpty(filter.PropertyName) ? "*" : SanitizeValue(filter.PropertyName))}" +
               $"{PROPS_JOIN}{(string.IsNullOrEmpty(filter.Operator) ? "*" : filter.Operator)}" +
               $"{PROPS_JOIN}{System.Net.WebUtility.UrlEncode(valueSerialized)}" +
               $"{PROPS_JOIN}{(filter.Visible.ToString().ToLower())}" +
               $"{PROPS_JOIN}{System.Net.WebUtility.UrlEncode(filter.Tag)}" +
               $"{PROPS_JOIN}{filtersSerialized}";
    }

    public static Filter Deserialize(string f)
    {
        const char PROPS_JOIN = ProjectionQueryQueryStringExtensions.FILTER_PROPERTIES_JOIN_CHARACTER;
        
        if (f.IndexOf(PROPS_JOIN, StringComparison.Ordinal) < 0)
        {
            if (FacetFilterShortcuts.ContainsKey(f))
            {
                return FacetFilterShortcuts[f];
            }
        }

        var propertyNameEnd = f.IndexOf($"{PROPS_JOIN}", StringComparison.Ordinal);
        var propertyName = DesanitizeValue(f.Substring(0, propertyNameEnd));

        var operatorEnd = f.IndexOf($"{PROPS_JOIN}", propertyNameEnd + 1, StringComparison.Ordinal);
        var operatorValue = f.Substring(propertyNameEnd + 1, operatorEnd - propertyNameEnd - 1);

        var valueEnd = f.IndexOf($"{PROPS_JOIN}", operatorEnd + 1, StringComparison.Ordinal);
        var value = f.Substring(operatorEnd + 1, valueEnd - operatorEnd - 1);

        var visibleEnd = f.IndexOf($"{PROPS_JOIN}", valueEnd + 1, StringComparison.Ordinal);
        var visible = f.Substring(valueEnd + 1, visibleEnd - valueEnd - 1) == "true";

        var tagEnd = f.IndexOf($"{PROPS_JOIN}", visibleEnd + 1, StringComparison.Ordinal);
        var tag = f.Substring(visibleEnd + 1, tagEnd - visibleEnd - 1);

        tag = System.Net.WebUtility.UrlDecode(tag);

        // replace back special serialization characters
        value = DesanitizeValue(value);

        var valueByShortcut = FacetValuesShortcuts.FirstOrDefault(v => v.Value == value);
        if (!string.IsNullOrEmpty(valueByShortcut.Key))
        {
            value = valueByShortcut.Key;
        }
        else if (!string.IsNullOrEmpty(value) && value.Length > 2 && value[0] == '#' && value[value.Length - 1] == '#')
        {
            valueByShortcut = FacetValuesShortcuts
                .FirstOrDefault(v => v.Value == $"({value.Replace("#", "")})");

            if (!string.IsNullOrEmpty(valueByShortcut.Key))
            {
                value = valueByShortcut.Key;
            }
        }

        var filters = new List<FilterConnector>();

        var filtersSerializedList = f.Substring(tagEnd + 1)
            .Split(ProjectionQueryQueryStringExtensions.FILTER_NESTED_FILTERS_JOIN_CHARACTER);
        
        if (filtersSerializedList.Length > 0)
        {
            filters = filtersSerializedList
                .Where(fs => fs.Length > 0)
                .Select(FilterConnectorQueryStringExtensions.Deserialize)
                .ToList();
        }

        var filter = new Filter()
        {
            PropertyName = propertyName,
            Operator = operatorValue,
            Tag = tag,
            Visible = visible,
            Filters = filters
        };

        if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
        {
            filter.Value = value[1..^1];
        }
        else
        {
            if (bool.TryParse(value, out var boolValue))
            {
                filter.Value = boolValue;
            }
            else if (Int64.TryParse(value, out var longValue))
            {
                // Always parse as Int64 — serialization loses type info,
                // and projection stores typically use long for integers.
                filter.Value = longValue;
            }
            else if (decimal.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out var decimalValue))
            {
                filter.Value = decimalValue;
            }
            else if (DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal, out var dateTimeValue))
            {
                filter.Value = dateTimeValue;
            }
            // Important: Guids may be stored via two ways: as simple strings or as Guid objects.
            // If we are here - that means guid was passed as an object, not as a string (see first `if` above).
            // So, if one would want to filter by string guid - they would send it in quotes ''.
            else if (Guid.TryParse(value, out var guidValue))
            {
                filter.Value = guidValue;
            }
        }

        return filter;
    }
}
