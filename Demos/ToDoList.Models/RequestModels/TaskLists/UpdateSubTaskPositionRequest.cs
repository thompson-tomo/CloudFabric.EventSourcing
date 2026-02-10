using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record UpdateSubTaskPositionRequest
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

    [Range(0, double.MaxValue)]
    public double Position { get; set; }
}