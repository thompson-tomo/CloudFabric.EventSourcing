using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskDueDateSet(
    Guid TaskListId,
    Guid TaskId,
    DateTime DueDate
) : Event(TaskId);