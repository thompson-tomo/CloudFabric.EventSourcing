using System.ComponentModel.DataAnnotations;
using ToDoList.Models.Validation;

namespace ToDoList.Models.RequestModels.TaskLists;

public record ShareTaskListRequest
{
    [Required]
    [NonEmptyGuid]
    public Guid? TaskListId { get; set; }

    [Required]
    [NonEmptyGuid]
    public Guid? SharedUserId { get; set; }

    [Required]
    [EmailAddress]
    public string SharedUserEmail { get; set; } = string.Empty;

    public bool CanEdit { get; set; }
}