using System;
using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record UpdateTaskDueDateRequest
{
    [Required]
    [NonEmptyGuid]
    public Guid? TaskListId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? TaskId { get; set; }

    public DateTime? DueDate { get; set; }
}