using System.Collections.Generic;

namespace ToDoList.Models.ViewModels.TaskLists;

public class TaskListViewModel
{
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public DateTime? UpdatedAt { get; set; }
    public double Position { get; set; }
    public int TasksCount { get; set; }
    public int OpenTasksCount { get; set; }
    public int ClosedTasksCount { get; set; }
    public List<TaskListShareViewModel> SharedUsers { get; set; } = new();
}