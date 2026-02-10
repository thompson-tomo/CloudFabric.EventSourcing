using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskListSharedWithUser(
    Guid TaskListId,
    Guid SharedUserId,
    string SharedUserEmail,
    bool CanEdit
) : Event(TaskListId);