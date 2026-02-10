using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskListShareRevoked(
    Guid TaskListId,
    Guid SharedUserId
) : Event(TaskListId);