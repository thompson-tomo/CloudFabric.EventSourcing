using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskAttachmentRemoved(
    Guid TaskListId,
    Guid TaskId,
    Guid AttachmentId
) : Event(TaskId);