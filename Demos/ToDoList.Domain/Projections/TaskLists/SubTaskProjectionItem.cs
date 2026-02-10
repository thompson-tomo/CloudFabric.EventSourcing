using CloudFabric.Projections.Attributes;

namespace ToDoList.Domain.Projections.TaskLists;

public class SubTaskProjectionItem
{
    [ProjectionDocumentProperty(IsFilterable = true)]
    public Guid Id { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true)]
    public string Title { get; set; } = string.Empty;

    [ProjectionDocumentProperty(IsSortable = true)]
    public double Position { get; set; }

    [ProjectionDocumentProperty(IsFilterable = true)]
    public bool IsCompleted { get; set; }
}