using System.Text.RegularExpressions;

namespace CloudFabric.Projections.OpenSearch.Extensions;

public static partial class StringExtensions
{
    private const string ESCAPE_LIST = @"[+\-=&|!(){}\[\]^""~*<>?:\\/]";
    internal static string EscapeElasticUnsupportedCharacters(this string instance)
    {
        return Regex.Replace(instance, ESCAPE_LIST, m => $@"\{m.Value}", RegexOptions.None, TimeSpan.FromMilliseconds(100));
    }
}
