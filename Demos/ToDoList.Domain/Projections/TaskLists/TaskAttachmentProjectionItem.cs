using CloudFabric.Projections.Attributes;

namespace ToDoList.Domain.Projections.TaskLists;

public class TaskAttachmentProjectionItem
{
    [ProjectionDocumentProperty(IsFilterable = true)]
    public Guid Id { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true)]
    public string OriginalFileName { get; set; } = string.Empty;

    [ProjectionDocumentProperty]
    public string StoredFilePath { get; set; } = string.Empty;

    [ProjectionDocumentProperty]
    public string ThumbnailFilePath { get; set; } = string.Empty;

    [ProjectionDocumentProperty(IsSortable = true)]
    public long SizeBytes { get; set; }

    [ProjectionDocumentProperty(IsSearchable = true)]
    public string ContentType { get; set; } = string.Empty;
}