using System;
using System.Collections.Generic;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using ToDoList.Domain.Events.TaskLists;

namespace ToDoList.Domain;

public class TaskList : AggregateBase
{
    private readonly Dictionary<Guid, TaskListShare> _shares = new();

    public string Name { get; protected set; }
    
    public Guid UserAccountId { get; protected set; }

    public override string PartitionKey => UserAccountId.ToString();

    public double Position { get; set; }

    public IReadOnlyCollection<TaskListShare> SharedUsers => _shares.Values;

    public TaskList(IEnumerable<IEvent> events) : base(events)
    {
    }

    public TaskList(Guid userAccountId, Guid id, string name)
        : base(id)
    {
        UserAccountId = userAccountId;
        Apply(new TaskListCreated(userAccountId, id, name));
    }

    public void UpdateName(string newName)
    {
        Apply(new TaskListNameUpdated(Id, newName));
    }

    public void UpdatePosition(int newPosition)
    {
        Apply(new TaskListPositionUpdated(Id, newPosition));
    }

    public void ShareWithUser(Guid sharedUserId, string sharedUserEmail, bool canEdit)
    {
        if (sharedUserId == UserAccountId)
        {
            throw new InvalidOperationException("Cannot share a task list with its owner.");
        }

        if (_shares.TryGetValue(sharedUserId, out var existingShare))
        {
            var emailMatches = string.Equals(existingShare.SharedUserEmail, sharedUserEmail, StringComparison.OrdinalIgnoreCase);

            if (emailMatches && existingShare.CanEdit == canEdit)
            {
                return;
            }
        }

        Apply(new TaskListSharedWithUser(Id, sharedUserId, sharedUserEmail, canEdit));
    }

    public void RevokeShare(Guid sharedUserId)
    {
        if (!_shares.ContainsKey(sharedUserId))
        {
            return;
        }

        Apply(new TaskListShareRevoked(Id, sharedUserId));
    }

    #region Event Handlers

    public void On(TaskListCreated @event)
    {
        UserAccountId = @event.UserAccountId;
        Name = @event.Name;
    }

    public void On(TaskListNameUpdated @event)
    {
        Name = @event.NewName;
    }
    
    public void On(TaskListPositionUpdated @event)
    {
        Position = @event.NewPosition;
    }

    public void On(TaskListSharedWithUser @event)
    {
        _shares[@event.SharedUserId] = new TaskListShare(@event.SharedUserId, @event.SharedUserEmail, @event.CanEdit);
    }

    public void On(TaskListShareRevoked @event)
    {
        _shares.Remove(@event.SharedUserId);
    }
    
    #endregion
}