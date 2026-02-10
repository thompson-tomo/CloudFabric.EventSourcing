using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record AddTaskAttachmentRequest
{
    [Required]
    [NonEmptyGuid]
    public Guid? TaskListId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? TaskId { get; set; }

    public Guid? AttachmentId { get; set; }

    [Required]
    [MaxLength(512)]
    public string OriginalFileName { get; set; } = string.Empty;

    [Required]
    [MaxLength(2048)]
    public string StoredFilePath { get; set; } = string.Empty;

    [Required]
    [MaxLength(2048)]
    public string ThumbnailFilePath { get; set; } = string.Empty;

    [Range(0, long.MaxValue)]
    public long SizeBytes { get; set; }

    [Required]
    [MaxLength(128)]
    public string ContentType { get; set; } = string.Empty;
}