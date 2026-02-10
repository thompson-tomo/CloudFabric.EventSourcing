using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskDueDateCleared(
    Guid TaskListId,
    Guid TaskId
) : Event(TaskId);