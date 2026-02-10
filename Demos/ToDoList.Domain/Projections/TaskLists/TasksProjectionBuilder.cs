using System.Collections.Generic;
using System.Linq;
using CloudFabric.Projections;
using ToDoList.Domain.Events.TaskLists;

namespace ToDoList.Domain.Projections.TaskLists;


public class TasksProjectionBuilder : ProjectionBuilder<TaskProjectionItem>,
    IHandleEvent<TaskCreated>,
    IHandleEvent<TaskTitleUpdated>,
    IHandleEvent<TaskCompletedStatusUpdated>,
    IHandleEvent<TaskPositionUpdated>,
    IHandleEvent<SubTaskAdded>,
    IHandleEvent<SubTaskTitleUpdated>,
    IHandleEvent<SubTaskCompletionToggled>,
    IHandleEvent<SubTaskPositionUpdated>,
    IHandleEvent<SubTaskRemoved>,
    IHandleEvent<TaskAttachmentAdded>,
    IHandleEvent<TaskAttachmentRemoved>,
    IHandleEvent<TaskDueDateSet>,
    IHandleEvent<TaskDueDateCleared>,
    IHandleEvent<TaskReminderScheduled>,
    IHandleEvent<TaskReminderCleared>
{
    public TasksProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory, 
        ProjectionOperationIndexSelector indexSelector
    ) : base(projectionRepositoryFactory, indexSelector)
    {
    }

    public async System.Threading.Tasks.Task On(TaskCreated evt)
    {
        await UpsertDocument(
            new TaskProjectionItem() 
            {
                Id = evt.AggregateId,
                Title = evt.Title,
                Description = evt.Description,
                UserAccountId = evt.UserAccountId,
                TaskListId = evt.TaskListId,
                IsCompleted = false,
                SubTasks = new List<SubTaskProjectionItem>(),
                Attachments = new List<TaskAttachmentProjectionItem>()
            },
            evt.PartitionKey,
            evt.Timestamp
        );
    }

    public async System.Threading.Tasks.Task On(TaskCompletedStatusUpdated evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) =>
            {
                projectionDocument.IsCompleted = evt.IsCompleted;
            }
        );
    }

    public async System.Threading.Tasks.Task On(TaskTitleUpdated evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) => 
            {
                projectionDocument.Title = evt.NewTitle;
            }
        );
    }
    
    public async System.Threading.Tasks.Task On(TaskPositionUpdated evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) =>
            {
                projectionDocument.TaskListId = evt.TaskListId;
                projectionDocument.Position = evt.NewPosition;
            }
        );
    }

    public async System.Threading.Tasks.Task On(SubTaskAdded evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.SubTasks ??= new List<SubTaskProjectionItem>();
                var existing = projectionDocument.SubTasks.FirstOrDefault(st => st.Id == evt.SubTaskId);

                if (existing == null)
                {
                    projectionDocument.SubTasks.Add(new SubTaskProjectionItem
                    {
                        Id = evt.SubTaskId,
                        Title = evt.Title,
                        Position = evt.Position,
                        IsCompleted = false
                    });
                }
                else
                {
                    existing.Title = evt.Title;
                    existing.Position = evt.Position;
                    existing.IsCompleted = false;
                }
            }
        );
    }

    public async System.Threading.Tasks.Task On(SubTaskTitleUpdated evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.SubTasks ??= new List<SubTaskProjectionItem>();
                var existing = projectionDocument.SubTasks.FirstOrDefault(st => st.Id == evt.SubTaskId);

                if (existing != null)
                {
                    existing.Title = evt.Title;
                }
            }
        );
    }

    public async System.Threading.Tasks.Task On(SubTaskCompletionToggled evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.SubTasks ??= new List<SubTaskProjectionItem>();
                var existing = projectionDocument.SubTasks.FirstOrDefault(st => st.Id == evt.SubTaskId);

                if (existing != null)
                {
                    existing.IsCompleted = evt.IsCompleted;
                }
            }
        );
    }

    public async System.Threading.Tasks.Task On(SubTaskPositionUpdated evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.SubTasks ??= new List<SubTaskProjectionItem>();
                var existing = projectionDocument.SubTasks.FirstOrDefault(st => st.Id == evt.SubTaskId);

                if (existing != null)
                {
                    existing.Position = evt.Position;
                }
            }
        );
    }

    public async System.Threading.Tasks.Task On(SubTaskRemoved evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.SubTasks ??= new List<SubTaskProjectionItem>();
                projectionDocument.SubTasks.RemoveAll(st => st.Id == evt.SubTaskId);
            }
        );
    }

    public async System.Threading.Tasks.Task On(TaskAttachmentAdded evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.Attachments ??= new List<TaskAttachmentProjectionItem>();
                var existing = projectionDocument.Attachments.FirstOrDefault(a => a.Id == evt.AttachmentId);

                if (existing == null)
                {
                    projectionDocument.Attachments.Add(new TaskAttachmentProjectionItem
                    {
                        Id = evt.AttachmentId,
                        OriginalFileName = evt.OriginalFileName,
                        StoredFilePath = evt.StoredFilePath,
                        ThumbnailFilePath = evt.ThumbnailFilePath,
                        SizeBytes = evt.SizeBytes,
                        ContentType = evt.ContentType
                    });
                }
                else
                {
                    existing.OriginalFileName = evt.OriginalFileName;
                    existing.StoredFilePath = evt.StoredFilePath;
                    existing.ThumbnailFilePath = evt.ThumbnailFilePath;
                    existing.SizeBytes = evt.SizeBytes;
                    existing.ContentType = evt.ContentType;
                }
            }
        );
    }

    public async System.Threading.Tasks.Task On(TaskAttachmentRemoved evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument =>
            {
                projectionDocument.Attachments ??= new List<TaskAttachmentProjectionItem>();
                projectionDocument.Attachments.RemoveAll(a => a.Id == evt.AttachmentId);
            }
        );
    }

    public async System.Threading.Tasks.Task On(TaskDueDateSet evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument => { projectionDocument.DueDate = evt.DueDate; }
        );
    }

    public async System.Threading.Tasks.Task On(TaskDueDateCleared evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument => { projectionDocument.DueDate = null; }
        );
    }

    public async System.Threading.Tasks.Task On(TaskReminderScheduled evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument => { projectionDocument.ReminderAt = evt.ReminderAt; }
        );
    }

    public async System.Threading.Tasks.Task On(TaskReminderCleared evt)
    {
        await UpdateDocument(
            evt.AggregateId,
            evt.PartitionKey,
            evt.Timestamp,
            projectionDocument => { projectionDocument.ReminderAt = null; }
        );
    }
    
    public async System.Threading.Tasks.Task On(AggregateUpdatedEvent<Task> @event)
    {
        await SetDocumentUpdatedAt(@event.AggregateId, @event.PartitionKey, @event.UpdatedAt);
    }
}