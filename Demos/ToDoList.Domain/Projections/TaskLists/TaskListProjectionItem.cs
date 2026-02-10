using System.Collections.Generic;
using CloudFabric.Projections;
using CloudFabric.Projections.Attributes;

namespace ToDoList.Domain.Projections.TaskLists;


[ProjectionDocument]
public class TaskListProjectionItem : ProjectionDocument
{
    [ProjectionDocumentProperty(IsSearchable = true, IsFilterable = true)]
    public Guid UserAccountId { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true)]
    public string Name { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true, IsFilterable = true)]
    public DateTime? CreatedAt { get; set; }
    
    [ProjectionDocumentProperty(IsSearchable = true, IsFilterable = true)]
    public DateTime? UpdatedAt { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true, IsFilterable = true)]
    public int TasksCount { get; set; }

    [ProjectionDocumentProperty]
    public int OpenTasksCount { get; set; }

    [ProjectionDocumentProperty]
    public int ClosedTasksCount { get; set; }
    
    [ProjectionDocumentProperty]
    public double Position { get; set; }

    [ProjectionDocumentProperty]
    public List<TaskListShareProjection> SharedUsers { get; set; } = new();
}