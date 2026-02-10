using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskReminderScheduled(
    Guid TaskListId,
    Guid TaskId,
    DateTime ReminderAt
) : Event(TaskId);