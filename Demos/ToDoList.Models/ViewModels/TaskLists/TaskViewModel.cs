using System;
using System.Collections.Generic;

namespace ToDoList.Models.ViewModels.TaskLists;

public class TaskViewModel
{
    public Guid Id { get; set; }
    public Guid TaskListId { get; set; }
    public string? Title { get; set; }
    public string? Description { get; set; }
    public double Position { get; set; }
    public bool IsClosed { get; set; }
    public DateTime? DueDate { get; set; }
    public DateTime? ReminderAt { get; set; }
    public List<SubTaskViewModel> SubTasks { get; set; } = new();
    public List<TaskAttachmentViewModel> Attachments { get; set; } = new();
}