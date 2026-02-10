using CloudFabric.EventSourcing.EventStore;

namespace ToDoList.Domain.Events.TaskLists;

public record TaskCompletedStatusUpdated(Guid TaskListId, Guid TaskId, bool IsCompleted) : Event;