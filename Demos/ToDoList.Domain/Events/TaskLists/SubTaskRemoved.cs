using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record SubTaskRemoved(
    Guid TaskListId,
    Guid TaskId,
    Guid SubTaskId
) : Event(TaskId);