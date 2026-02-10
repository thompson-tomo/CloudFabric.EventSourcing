using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskAttachmentAdded(
    Guid TaskListId,
    Guid TaskId,
    Guid AttachmentId,
    string OriginalFileName,
    string StoredFilePath,
    string ThumbnailFilePath,
    long SizeBytes,
    string ContentType
) : Event(TaskId);