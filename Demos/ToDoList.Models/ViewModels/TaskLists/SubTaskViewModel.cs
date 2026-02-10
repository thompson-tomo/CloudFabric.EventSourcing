using System;

namespace ToDoList.Models.ViewModels.TaskLists;

public class SubTaskViewModel
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public double Position { get; set; }
    public bool IsCompleted { get; set; }
}