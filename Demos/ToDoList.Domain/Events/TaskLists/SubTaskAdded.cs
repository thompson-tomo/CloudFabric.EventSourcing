using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record SubTaskAdded(
    Guid TaskListId,
    Guid TaskId,
    Guid SubTaskId,
    string Title,
    double Position
) : Event(TaskId);