using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record SubTaskPositionUpdated(
    Guid TaskListId,
    Guid TaskId,
    Guid SubTaskId,
    double Position
) : Event(TaskId);