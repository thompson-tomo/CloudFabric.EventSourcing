using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record SubTaskTitleUpdated(
    Guid TaskListId,
    Guid TaskId,
    Guid SubTaskId,
    string Title
) : Event(TaskId);