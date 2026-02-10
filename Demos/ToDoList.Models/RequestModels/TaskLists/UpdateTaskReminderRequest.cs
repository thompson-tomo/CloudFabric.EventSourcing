using System;
using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record UpdateTaskReminderRequest
{
    [Required]
    [NonEmptyGuid]
    public Guid? TaskListId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? TaskId { get; set; }

    public DateTime? ReminderAt { get; set; }
}