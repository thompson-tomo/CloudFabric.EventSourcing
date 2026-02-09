namespace CloudFabric.Projections.Exceptions;

public class ProjectionQueryFilterException : Exception
{
    public ProjectionQueryFilterException(string filterProperty, string projectionSchemaName, Exception? innerException = null)
        : base(
            $"Failed to query documents: provided filter property {filterProperty} does not exist on projection schema {projectionSchemaName}",
            innerException
        )
    {
    }
}