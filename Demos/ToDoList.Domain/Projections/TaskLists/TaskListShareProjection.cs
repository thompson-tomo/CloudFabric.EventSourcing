using CloudFabric.Projections.Attributes;

namespace ToDoList.Domain.Projections.TaskLists;

public class TaskListShareProjection
{
    [ProjectionDocumentProperty(IsFilterable = true)]
    public Guid SharedUserId { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true)]
    public string SharedUserEmail { get; set; } = string.Empty;

    [ProjectionDocumentProperty]
    public bool CanEdit { get; set; }
}