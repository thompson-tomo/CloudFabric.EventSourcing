using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record RemoveSubTaskRequest
{
    [Required]
    [NonEmptyGuid]
    public Guid? TaskListId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? TaskId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? SubTaskId { get; set; }
}