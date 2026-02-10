using System;

namespace ToDoList.Models.ViewModels.TaskLists;

public class TaskListShareViewModel
{
    public Guid SharedUserId { get; set; }
    public string SharedUserEmail { get; set; } = string.Empty;
    public bool CanEdit { get; set; }
}