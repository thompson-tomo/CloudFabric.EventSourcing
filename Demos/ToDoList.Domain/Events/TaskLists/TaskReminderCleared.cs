using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskReminderCleared(
    Guid TaskListId,
    Guid TaskId
) : Event(TaskId);