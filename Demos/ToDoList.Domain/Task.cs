using System;
using System.Collections.Generic;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using ToDoList.Domain.Events.TaskLists;

namespace ToDoList.Domain;

public class Task : AggregateBase
{
    private readonly Dictionary<Guid, SubTaskItem> _subTasks = new();
    private readonly Dictionary<Guid, TaskAttachment> _attachments = new();

    public Guid UserAccountId { get; protected set; }

    public override string PartitionKey => UserAccountId.ToString();

    public Guid TaskListId { get; protected set; }

    public string? Title { get; protected set; }

    public string? Description { get; protected set; }

    public double Position { get; protected set; }

    public bool IsCompleted { get; protected set; }

    public DateTime? DueDate { get; protected set; }

    public DateTime? ReminderAt { get; protected set; }

    public IReadOnlyCollection<SubTaskItem> SubTasks => _subTasks.Values;

    public IReadOnlyCollection<TaskAttachment> Attachments => _attachments.Values;

    public Task(IEnumerable<IEvent> events) : base(events)
    {
    }

    public Task(Guid userAccountId, Guid taskListId, Guid taskId, string title, string? description)
        : base(taskId)
    {
        UserAccountId = userAccountId;
        Apply(new TaskCreated(userAccountId, taskListId, taskId, title, description));
    }

    public void UpdateTitle(string newTitle)
    {
        Apply(new TaskTitleUpdated(TaskListId, Id, newTitle));
    }

    public void SetCompletedStatus(bool newCompletedStatus)
    {
        Apply(new TaskCompletedStatusUpdated(TaskListId, Id, newCompletedStatus));
    }

    public void UpdatePosition(Guid newTaskListId, double newPosition)
    {
        Apply(new TaskPositionUpdated(TaskListId, newTaskListId, Id, IsCompleted, newPosition));
    }

    public void AddSubTask(Guid subTaskId, string title, double position)
    {
        if (_subTasks.ContainsKey(subTaskId))
        {
            throw new InvalidOperationException("Subtask already exists on this task.");
        }

        Apply(new SubTaskAdded(TaskListId, Id, subTaskId, title, position));
    }

    public void UpdateSubTaskTitle(Guid subTaskId, string title)
    {
        if (!_subTasks.ContainsKey(subTaskId))
        {
            throw new InvalidOperationException("Subtask does not exist on this task.");
        }

        Apply(new SubTaskTitleUpdated(TaskListId, Id, subTaskId, title));
    }

    public void UpdateSubTaskPosition(Guid subTaskId, double position)
    {
        if (!_subTasks.ContainsKey(subTaskId))
        {
            throw new InvalidOperationException("Subtask does not exist on this task.");
        }

        Apply(new SubTaskPositionUpdated(TaskListId, Id, subTaskId, position));
    }

    public void ToggleSubTaskCompletion(Guid subTaskId, bool isCompleted)
    {
        if (!_subTasks.ContainsKey(subTaskId))
        {
            throw new InvalidOperationException("Subtask does not exist on this task.");
        }

        Apply(new SubTaskCompletionToggled(TaskListId, Id, subTaskId, isCompleted));
    }

    public void RemoveSubTask(Guid subTaskId)
    {
        if (!_subTasks.ContainsKey(subTaskId))
        {
            return;
        }

        Apply(new SubTaskRemoved(TaskListId, Id, subTaskId));
    }

    public void AddAttachment(
        Guid attachmentId,
        string originalFileName,
        string storedFilePath,
        string thumbnailFilePath,
        long sizeBytes,
        string contentType
    )
    {
        if (_attachments.ContainsKey(attachmentId))
        {
            throw new InvalidOperationException("Attachment already exists on this task.");
        }

        Apply(new TaskAttachmentAdded(TaskListId, Id, attachmentId, originalFileName, storedFilePath, thumbnailFilePath, sizeBytes, contentType));
    }

    public void RemoveAttachment(Guid attachmentId)
    {
        if (!_attachments.ContainsKey(attachmentId))
        {
            return;
        }

        Apply(new TaskAttachmentRemoved(TaskListId, Id, attachmentId));
    }

    public void SetDueDate(DateTime dueDate)
    {
        if (DueDate.HasValue && DueDate.Value == dueDate)
        {
            return;
        }

        Apply(new TaskDueDateSet(TaskListId, Id, dueDate));
    }

    public void ClearDueDate()
    {
        if (!DueDate.HasValue)
        {
            return;
        }

        Apply(new TaskDueDateCleared(TaskListId, Id));
    }

    public void ScheduleReminder(DateTime reminderAt)
    {
        if (ReminderAt.HasValue && ReminderAt.Value == reminderAt)
        {
            return;
        }

        Apply(new TaskReminderScheduled(TaskListId, Id, reminderAt));
    }

    public void ClearReminder()
    {
        if (!ReminderAt.HasValue)
        {
            return;
        }

        Apply(new TaskReminderCleared(TaskListId, Id));
    }

    #region Event Handlers

    public void On(TaskCreated @event)
    {
        TaskListId = @event.TaskListId;
        UserAccountId = @event.UserAccountId;
        Title = @event.Title;
        Description = @event.Description;
    }

    public void On(TaskTitleUpdated @event)
    {
        Title = @event.NewTitle;
    }

    public void On(TaskCompletedStatusUpdated @event)
    {
        IsCompleted = @event.IsCompleted;
    }

    public void On(TaskPositionUpdated @event)
    {
        TaskListId = @event.TaskListId;
        Position = @event.NewPosition;
    }

    public void On(SubTaskAdded @event)
    {
        _subTasks[@event.SubTaskId] = new SubTaskItem(@event.SubTaskId, @event.Title, @event.Position);
    }

    public void On(SubTaskTitleUpdated @event)
    {
        if (_subTasks.TryGetValue(@event.SubTaskId, out var subTask))
        {
            subTask.UpdateTitle(@event.Title);
        }
    }

    public void On(SubTaskPositionUpdated @event)
    {
        if (_subTasks.TryGetValue(@event.SubTaskId, out var subTask))
        {
            subTask.UpdatePosition(@event.Position);
        }
    }

    public void On(SubTaskCompletionToggled @event)
    {
        if (_subTasks.TryGetValue(@event.SubTaskId, out var subTask))
        {
            subTask.SetCompleted(@event.IsCompleted);
        }
    }

    public void On(SubTaskRemoved @event)
    {
        _subTasks.Remove(@event.SubTaskId);
    }

    public void On(TaskAttachmentAdded @event)
    {
        _attachments[@event.AttachmentId] = new TaskAttachment(
            @event.AttachmentId,
            @event.OriginalFileName,
            @event.StoredFilePath,
            @event.ThumbnailFilePath,
            @event.SizeBytes,
            @event.ContentType
        );
    }

    public void On(TaskAttachmentRemoved @event)
    {
        _attachments.Remove(@event.AttachmentId);
    }

    public void On(TaskDueDateSet @event)
    {
        DueDate = @event.DueDate;
    }

    public void On(TaskDueDateCleared @event)
    {
        DueDate = null;
    }

    public void On(TaskReminderScheduled @event)
    {
        ReminderAt = @event.ReminderAt;
    }

    public void On(TaskReminderCleared @event)
    {
        ReminderAt = null;
    }

    #endregion
}