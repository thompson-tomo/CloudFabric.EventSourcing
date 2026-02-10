using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record SubTaskCompletionToggled(
    Guid TaskListId,
    Guid TaskId,
    Guid SubTaskId,
    bool IsCompleted
) : Event(TaskId);