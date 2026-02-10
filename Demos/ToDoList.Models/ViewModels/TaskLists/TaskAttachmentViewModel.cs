using System;

namespace ToDoList.Models.ViewModels.TaskLists;

public class TaskAttachmentViewModel
{
    public Guid Id { get; set; }
    public string OriginalFileName { get; set; } = string.Empty;
    public string StoredFilePath { get; set; } = string.Empty;
    public string ThumbnailFilePath { get; set; } = string.Empty;
    public long SizeBytes { get; set; }
    public string ContentType { get; set; } = string.Empty;
}