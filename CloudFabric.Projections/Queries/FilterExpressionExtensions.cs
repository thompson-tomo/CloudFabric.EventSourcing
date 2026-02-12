using System.Linq.Expressions;
using System.Reflection;

namespace CloudFabric.Projections.Queries;

public static class FilterExpressionExtensions {

    /// <summary>
    /// Converts this filter object to Lambda Expression which can be used for linq.
    /// </summary>
    /// <param name="parameter">lambda argument - an object which will be passed to this expression and available for filtering.</param>
    /// <typeparam name="TParameter">type of the argument</typeparam>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public static Expression<Func<TParameter, bool>> ToLambdaExpression<TParameter>(this Filter filter, ParameterExpression? parameter = null)
    {
        var (expression, expressionParameter) = filter.ToExpression<TParameter>(parameter);
        return (Expression<Func<TParameter, bool>>)Expression.Lambda(expression, expressionParameter);
    }

    # region Reflection methods cache
    private static readonly MethodInfo StringStartsWith = typeof(string).GetMethods()
        .First(m => m.Name == "StartsWith" && m.GetParameters().Length == 1);
    private static readonly MethodInfo StringStartsWithIgnoreCaseArgument = typeof(string).GetMethods()
        .First(m => m.Name == "StartsWith" && m.GetParameters().Length == 3);
    private static readonly MethodInfo StringStartsWithStringComparisonArgument = typeof(string).GetMethods()
        .First(m => m.Name == "StartsWith" && m.GetParameters().Length == 2);

    private static readonly MethodInfo StringEndsWith = typeof(string).GetMethods()
        .First(m => m.Name == "EndsWith" && m.GetParameters().Length == 1);
    private static readonly MethodInfo StringEndsWithIgnoreCaseArgument = typeof(string).GetMethods()
        .First(m => m.Name == "EndsWith" && m.GetParameters().Length == 3);
    private static readonly MethodInfo StringEndsWithStringComparisonArgument = typeof(string).GetMethods()
        .First(m => m.Name == "EndsWith" && m.GetParameters().Length == 2);

    private static readonly MethodInfo StringContains = typeof(string).GetMethods()
        .First(m => m.Name == "Contains" && m.GetParameters().Length == 1);
    private static readonly MethodInfo StringContainsStringComparisonArgument = typeof(string).GetMethods()
        .First(m => m.Name == "Contains" && m.GetParameters().Length == 2);
    private static readonly MethodInfo ArrayIndexOf = typeof(Array).GetMethods()
        .First(m => m.Name == "IndexOf" && m.GetParameters().Length == 2);
    private static readonly MethodInfo ArrayExists = typeof(Array).GetMethods()
        .First(m => m.Name == "Exists" && m.GetParameters().Length == 2);

    private static readonly MethodInfo EnumerableToArray = typeof(System.Linq.Enumerable).GetMethod("ToArray");
    #endregion

    public static (Expression, ParameterExpression) ToExpression<TParameter>(this Filter filter, ParameterExpression? parameter = null)
    {
        if (string.IsNullOrEmpty(filter.PropertyName) || filter.PropertyName == "*")
        {
            var (firstFilterExpression, firstFilterParameter) = filter.Filters.First().Filter.ToExpression<TParameter>(parameter);
            
            foreach (var f in filter.Filters.Skip(1))
            {
                var (filterExpression, filterParameter) = f.Filter.ToExpression<TParameter>(parameter);
                firstFilterExpression = f.Logic switch
                {
                    FilterLogic.And => Expression.AndAlso(firstFilterExpression, filterExpression),
                    FilterLogic.Or => Expression.OrElse(firstFilterExpression, filterExpression),
                    _ => firstFilterExpression
                };
            }
            return (firstFilterExpression, firstFilterParameter);
        }

        parameter ??= Expression.Parameter(typeof(TParameter), "document");

        Expression property;

        if (typeof(TParameter).Name.Contains("Dictionary"))
        {
            var dictionaryIndexProperty = typeof(TParameter).GetProperty("Item");

            if (filter.PropertyName.Contains('.')) // nested property access, need to construct ["propertyName"]["propertyName"] 
            {                                      
                var propertyNamesPath = filter.PropertyName.Split('.');
                var propertyType = typeof(TParameter).GetGenericArguments()[1];

                property = Expression.MakeIndex(
                    parameter, 
                    dictionaryIndexProperty,
                    new[] { Expression.Constant(propertyNamesPath.First()) }
                );
                
                foreach (var prop in propertyNamesPath.Skip(1))
                {
                    // At runtime, the nested value can be either:
                    // 1. A Dictionary<string, object?> (nested object, e.g. EntityReference with denormalized data)
                    // 2. A List/Array (nested array, e.g. CategoryPaths, LocalizedText)
                    // We generate a conditional expression that checks the type at runtime.
                    var nestedDictType = typeof(Dictionary<string, object?>);
                    var isDictCheck = Expression.TypeIs(property, nestedDictType);

                    // Dictionary branch: cast to dict, index by property name, compare with filter value
                    var dictCast = Expression.Convert(property, nestedDictType);
                    var dictValue = Expression.MakeIndex(
                        dictCast,
                        nestedDictType.GetProperty("Item"),
                        new[] { Expression.Constant(prop) }
                    );
                    var dictComparison = ToExpression(dictValue, filter.Operator, filter.Value);

                    // Array branch: use Array.Exists with predicate
                    var arrayFilterPredicateParameter = Expression.Parameter(typeof(object), "i");
                    var arrayFilterPredicate = ToExpression(arrayFilterPredicateParameter, filter.Operator, filter.Value, prop);
                    var arrayComparison = ConstructArrayExistsExpressionFromFilter(
                        property,
                        arrayFilterPredicateParameter,
                        arrayFilterPredicate
                    );

                    // Runtime conditional: if value is Dictionary → dict comparison, else → array exists
                    property = Expression.Condition(isDictCheck, dictComparison, arrayComparison);
                    return (property, parameter);
                }
            }
            else // simple dictionary index expression like ["propertyName"]
            {
                property = Expression.MakeIndex(
                    parameter, 
                    dictionaryIndexProperty,
                    new[] { Expression.Constant(filter.PropertyName) }
                );
            }
        }
        else
        {
            property = Expression.PropertyOrField(parameter, filter.PropertyName);
        }

        var value = Expression.Constant(filter.Value);
        var operand = filter.Value == null ? (Expression)property : (Expression)Expression.Convert(property, filter.Value.GetType());

        Expression thisExpression = ToExpression(property, filter.Operator, filter.Value);
        // Expression thisExpression = filter.Operator switch
        // {
        //     FilterOperator.Equal => Expression.Equal(operand, value),
        //     FilterOperator.NotEqual => Expression.NotEqual(operand, value),
        //     FilterOperator.Greater => Expression.GreaterThan(operand, value),
        //     FilterOperator.GreaterOrEqual => Expression.GreaterThanOrEqual(operand, value),
        //     FilterOperator.Lower => Expression.LessThan(operand, value),
        //     FilterOperator.LowerOrEqual => Expression.LessThanOrEqual(operand, value),
        //     FilterOperator.StartsWith => Expression.Call(operand, StringStartsWith, value),
        //     FilterOperator.EndsWith => Expression.Call(operand, StringEndsWith, value),
        //     FilterOperator.Contains => Expression.Call(operand, StringContains, value),
        //     FilterOperator.StartsWithIgnoreCase => Expression.Call(operand, StringStartsWithStringComparisonArgument, value, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
        //     FilterOperator.EndsWithIgnoreCase => Expression.Call(operand, StringEndsWithStringComparisonArgument, value, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
        //     FilterOperator.ContainsIgnoreCase => Expression.Call(operand, StringContainsStringComparisonArgument, value, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
        //     FilterOperator.ArrayContains => ConstructArrayContainsExpressionFromFilter(property, value),
        //     _ => throw new Exception(
        //         $"Cannot create an expression. Filter's operator is either incorrect or not supported: {filter.Operator}"
        //     )
        // };

        if (filter.Filters.Count <= 0)
        {
            return (thisExpression, parameter);
        }

        foreach (var f in filter.Filters)
        {
            var (filterExpression, filterParameter) = f.Filter.ToExpression<TParameter>(parameter);
            thisExpression = f.Logic switch
            {
                FilterLogic.And => Expression.AndAlso(thisExpression, filterExpression),
                FilterLogic.Or => Expression.OrElse(thisExpression, filterExpression),
                _ => thisExpression
            };
        }

        return (thisExpression, parameter);
    }

    private static Expression ToExpression(Expression parameter, string oper, object? value, string? parameterAccessor = null)
    {
        var valueExpression = Expression.Constant(value);
        Expression operand;

        if (parameterAccessor != null)
        {
            // parameter is an array element (object) — cast to Dictionary first, then index by property name
            var castExpression = Expression.Convert(parameter, typeof(Dictionary<string, object?>));
            operand = Expression.MakeIndex(
                castExpression,
                typeof(Dictionary<string, object?>).GetProperty("Item"),
                new[] { Expression.Constant(parameterAccessor) }
            );
            // Convert the indexed value (object?) to the filter value type for comparison
            if (value != null)
            {
                operand = Expression.Convert(operand, value.GetType());
            }
        }
        else
        {
            operand = value == null ? (Expression)parameter : (Expression)Expression.Convert(parameter, value.GetType());
        }

        Expression thisExpression = oper switch
        {
            FilterOperator.Equal => Expression.Equal(operand, valueExpression),
            FilterOperator.NotEqual => Expression.NotEqual(operand, valueExpression),
            FilterOperator.Greater => Expression.GreaterThan(operand, valueExpression),
            FilterOperator.GreaterOrEqual => Expression.GreaterThanOrEqual(operand, valueExpression),
            FilterOperator.Lower => Expression.LessThan(operand, valueExpression),
            FilterOperator.LowerOrEqual => Expression.LessThanOrEqual(operand, valueExpression),
            FilterOperator.StartsWith => Expression.Call(operand, StringStartsWith, valueExpression),
            FilterOperator.EndsWith => Expression.Call(operand, StringEndsWith, valueExpression),
            FilterOperator.Contains => Expression.Call(operand, StringContains, valueExpression),
            FilterOperator.StartsWithIgnoreCase => Expression.Call(operand, StringStartsWithStringComparisonArgument, valueExpression, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
            FilterOperator.EndsWithIgnoreCase => Expression.Call(operand, StringEndsWithStringComparisonArgument, valueExpression, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
            FilterOperator.ContainsIgnoreCase => Expression.Call(operand, StringContainsStringComparisonArgument, valueExpression, Expression.Constant(StringComparison.InvariantCultureIgnoreCase)),
            FilterOperator.ArrayContains => ConstructArrayContainsExpressionFromFilter(parameter, valueExpression),
            _ => throw new Exception(
                $"Cannot create an expression. Filter's operator is either incorrect or not supported: {oper}"
            )
        };

        return thisExpression;
    }

    private static Expression ConstructArrayContainsExpressionFromFilter(Expression arrayProperty, Expression value)
    {
        var castExpression = Expression.Convert(arrayProperty, typeof(IEnumerable<>).MakeGenericType(value.Type));
        var toArrayMethodCallExpression = Expression.Call(EnumerableToArray.MakeGenericMethod(value.Type), castExpression);
        var indexOfMethodCallExpression = Expression.Call(ArrayIndexOf, (Expression)toArrayMethodCallExpression, value);

        return Expression.GreaterThan(indexOfMethodCallExpression, Expression.Constant(-1));
    }
    
    private static Expression ConstructArrayExistsExpressionFromFilter(
        Expression arrayProperty, 
        ParameterExpression predicateParameter, 
        Expression predicate
    )
    {
        var arrayElementType = typeof(Dictionary<string, object?>);
        
        var castExpression = Expression.Convert(arrayProperty, typeof(IEnumerable<>).MakeGenericType(typeof(object)));
        var toArrayMethodCallExpression = Expression.Call(EnumerableToArray.MakeGenericMethod(typeof(object)), castExpression);

        Type predicateType = typeof(Predicate<>).MakeGenericType(typeof(object));

        //var parameter = Expression.Parameter(typeof(object), "i");
        var t = Expression.Lambda(predicateType, predicate, new [] { predicateParameter } );
        
        var callExpression = Expression.Call(ArrayExists.MakeGenericMethod(typeof(object)), (Expression)toArrayMethodCallExpression, t);

        return callExpression;
    }

    public static Filter Where<T>(Expression<Func<T, bool>> expression, string tag = "")
    {
        if (expression.NodeType == ExpressionType.Lambda)
        {
            var f = ConstructFilterFromExpression(expression.Body);

            f.Tag = tag;

            return f;
        }

        throw new Exception($"Unsupported expression type: {expression.NodeType}");
    }

    public static object? GetExpressionValue(Expression expression)
    {
        if (expression.NodeType == ExpressionType.Constant)
        {
            return (expression as ConstantExpression)?.Value;
        }
        else if (expression.NodeType == ExpressionType.MemberAccess)
        {
            MemberInfo? memberInfo = (expression as MemberExpression)?.Member;

            if (memberInfo?.MemberType == MemberTypes.Field)
            {
                FieldInfo fieldInfo = (FieldInfo)memberInfo;
                var expressionValue = GetExpressionValue((expression as MemberExpression).Expression);
                var fieldValue = fieldInfo.GetValue(expressionValue);
                return fieldValue;
            }
            else if (memberInfo?.MemberType == MemberTypes.Property)
            {
                if ((expression as MemberExpression).Expression.NodeType == ExpressionType.Parameter)
                {
                    return (expression as MemberExpression).Member.Name;
                }

                // property is just a wrapper around field right?
                PropertyInfo propertyInfo = (PropertyInfo)memberInfo;
                var propertyValue = GetExpressionValue((expression as MemberExpression).Expression);
                var fieldValue = propertyInfo.GetValue(propertyValue);

                // TODO@sergey: nested objects member access
                return fieldValue;
            }
            else
            {
                throw new Exception($"Expression member type is not supported: {memberInfo.MemberType}");
            }
        }
        else if (expression.NodeType == ExpressionType.Call)
        {
            var methodCallExpression = (MethodCallExpression)expression;

            object[] args = methodCallExpression.Arguments.Select(e => GetExpressionValue(e)).ToArray();
            var obj = methodCallExpression.Object == null ? null : GetExpressionValue(methodCallExpression.Object);
            var result = methodCallExpression.Method.Invoke(obj, args);
            return result;
        }
        else if (expression.NodeType == ExpressionType.Convert)
        {
            var targetType = (expression as UnaryExpression).Type;
            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                targetType = Nullable.GetUnderlyingType(targetType);
            }

            var operand = (expression as UnaryExpression).Operand;
            object expressionValue = GetExpressionValue(operand);

            try
            {
                return Convert.ChangeType(expressionValue, targetType);
            }
            catch (Exception)
            {
            }

            return expressionValue;
        }
        else
        {
            throw new Exception($"Cannot get value from expression of type {expression.NodeType.ToString()}");
        }
    }

    private static Filter ConstructFilterFromExpression(Expression expression, string propertyPath = "")
    {
        switch (expression.NodeType)
        {
            case ExpressionType.Equal:
                var filterEq = new Filter();
                filterEq.Operator = FilterOperator.Equal;
                filterEq.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterEq.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterEq;
            case ExpressionType.NotEqual:
                var filterNEq = new Filter();
                filterNEq.Operator = FilterOperator.NotEqual;
                filterNEq.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterNEq.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterNEq;
            case ExpressionType.GreaterThan:
                var filterGt = new Filter();
                filterGt.Operator = FilterOperator.Greater;
                filterGt.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterGt.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterGt;
            case ExpressionType.GreaterThanOrEqual:
                var filterGe = new Filter();
                filterGe.Operator = FilterOperator.GreaterOrEqual;
                filterGe.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterGe.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterGe;
            case ExpressionType.LessThan:
                var filterLt = new Filter();
                filterLt.Operator = FilterOperator.Lower;
                filterLt.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterLt.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterLt;
            case ExpressionType.LessThanOrEqual:
                var filterLe = new Filter();
                filterLe.Operator = FilterOperator.LowerOrEqual;
                filterLe.PropertyName = propertyPath + GetExpressionValue((expression as BinaryExpression).Left).ToString();
                filterLe.Value = GetExpressionValue((expression as BinaryExpression).Right);
                return filterLe;
            case ExpressionType.AndAlso:
                var leftAnd = ConstructFilterFromExpression((expression as BinaryExpression).Left, propertyPath);
                var rightAnd = ConstructFilterFromExpression((expression as BinaryExpression).Right, propertyPath);

                return leftAnd.And(rightAnd);
            case ExpressionType.OrElse:
                var leftOr = ConstructFilterFromExpression((expression as BinaryExpression).Left, propertyPath);
                var rightOr = ConstructFilterFromExpression((expression as BinaryExpression).Right, propertyPath);

                return leftOr.Or(rightOr);
            case ExpressionType.Call:
                var e = expression as MethodCallExpression;
                
                if (e.Method.DeclaringType == typeof(string))
                {
                    var property = (string)GetExpressionValue(e.Object);
                    
                    if (e.Method.Name == "StartsWith")
                    {
                        var filterOperator = FilterOperator.StartsWith;

                        // this is for public bool StartsWith(string value, StringComparison comparisonType)
                        // overload
                        if (e.Arguments.Count == 2)
                        {
                            switch ((StringComparison)GetExpressionValue(e.Arguments[1])!)
                            {
                                case StringComparison.Ordinal:
                                case StringComparison.CurrentCulture:
                                case StringComparison.InvariantCulture:
                                    filterOperator = FilterOperator.StartsWith;
                                    break;
                                case StringComparison.OrdinalIgnoreCase:
                                case StringComparison.CurrentCultureIgnoreCase:
                                case StringComparison.InvariantCultureIgnoreCase:
                                    filterOperator = FilterOperator.StartsWithIgnoreCase;
                                    break;
                            }
                        }
                        // this is for public bool StartsWith(string value, bool ignoreCase, CultureInfo? culture)
                        // overload
                        else if (e.Arguments.Count == 3)
                        {
                            switch ((bool)GetExpressionValue(e.Arguments[1])!)
                            {
                                case false:
                                    filterOperator = FilterOperator.StartsWith;
                                    break;
                                case true:
                                    filterOperator = FilterOperator.StartsWithIgnoreCase;
                                    break;
                            }
                        }

                        return new Filter()
                        {
                            Operator = filterOperator,
                            PropertyName = property,
                            Value = GetExpressionValue(e.Arguments.First())
                        };
                    }
                    else if (e.Method.Name == "EndsWith")
                    {
                        var filterOperator = FilterOperator.EndsWith;

                        // this is for public bool EndsWith(string value, StringComparison comparisonType)
                        // overload
                        if (e.Arguments.Count == 2)
                        {
                            switch ((StringComparison)GetExpressionValue(e.Arguments[1])!)
                            {
                                case StringComparison.Ordinal:
                                case StringComparison.CurrentCulture:
                                case StringComparison.InvariantCulture:
                                    filterOperator = FilterOperator.EndsWith;
                                    break;
                                case StringComparison.OrdinalIgnoreCase:
                                case StringComparison.CurrentCultureIgnoreCase:
                                case StringComparison.InvariantCultureIgnoreCase:
                                    filterOperator = FilterOperator.EndsWithIgnoreCase;
                                    break;
                            }
                        }
                        // this is for public bool EndsWith(string value, bool ignoreCase, CultureInfo? culture)
                        // overload
                        else if (e.Arguments.Count == 3)
                        {
                            switch ((bool)GetExpressionValue(e.Arguments[1])!)
                            {
                                case false:
                                    filterOperator = FilterOperator.EndsWith;
                                    break;
                                case true:
                                    filterOperator = FilterOperator.EndsWithIgnoreCase;
                                    break;
                            }
                        }

                        return new Filter()
                        {
                            Operator = filterOperator,
                            PropertyName = property,
                            Value = GetExpressionValue(e.Arguments.First())
                        };
                    }
                    else if (e.Method.Name == "Contains")
                    {
                        var filterOperator = FilterOperator.Contains;

                        // this is for public bool StartsWith(string value, StringComparison comparisonType)
                        // overload
                        if (e.Arguments.Count == 2)
                        {
                            switch ((StringComparison)GetExpressionValue(e.Arguments[1])!)
                            {
                                case StringComparison.Ordinal:
                                case StringComparison.CurrentCulture:
                                case StringComparison.InvariantCulture:
                                    filterOperator = FilterOperator.Contains;
                                    break;
                                case StringComparison.OrdinalIgnoreCase:
                                case StringComparison.CurrentCultureIgnoreCase:
                                case StringComparison.InvariantCultureIgnoreCase:
                                    filterOperator = FilterOperator.ContainsIgnoreCase;
                                    break;
                            }
                        }

                        return new Filter()
                        {
                            Operator = filterOperator,
                            PropertyName = property,
                            Value = GetExpressionValue(e.Arguments.First())
                        };
                    }
                    else
                    {
                        throw new Exception($"Unsupported method call on String in expression: {e.Method.Name}");
                    }
                }
                else if (e.Method.DeclaringType == typeof(System.Linq.Enumerable))
                {
                    if (e.Method.Name == "Any")
                    {
                        var collectionName = propertyPath + ((expression as MethodCallExpression).Arguments[0] as MemberExpression).Member.Name;
                        return ConstructFilterFromExpression(((expression as MethodCallExpression).Arguments[1] as LambdaExpression).Body, $"{collectionName}.");
                    }
                    else
                    {
                        throw new Exception($"Unsupported method call on Enumerable in expression: {e.Method.Name}");
                    }
                }
                
                throw new Exception($"Method call on unsupported object: {e.Method.DeclaringType} in expression.");
            default:
                throw new Exception($"Unsupported expression type: {expression.NodeType}");
        }
    }
}