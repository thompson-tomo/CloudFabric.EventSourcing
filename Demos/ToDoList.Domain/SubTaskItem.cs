namespace ToDoList.Domain;

public class SubTaskItem
{
    public SubTaskItem(Guid id, string title, double position)
    {
        Id = id;
        Title = title;
        Position = position;
    }

    public Guid Id { get; }

    public string Title { get; private set; }

    public double Position { get; private set; }

    public bool IsCompleted { get; private set; }

    public void UpdateTitle(string title)
    {
        Title = title;
    }

    public void UpdatePosition(double position)
    {
        Position = position;
    }

    public void SetCompleted(bool isCompleted)
    {
        IsCompleted = isCompleted;
    }
}