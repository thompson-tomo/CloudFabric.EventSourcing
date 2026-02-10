namespace ToDoList.Domain;

public class TaskListShare
{
    public TaskListShare(Guid sharedUserId, string sharedUserEmail, bool canEdit)
    {
        SharedUserId = sharedUserId;
        SharedUserEmail = sharedUserEmail;
        CanEdit = canEdit;
    }

    public Guid SharedUserId { get; }

    public string SharedUserEmail { get; private set; }

    public bool CanEdit { get; private set; }

    public void Update(string sharedUserEmail, bool canEdit)
    {
        SharedUserEmail = sharedUserEmail;
        CanEdit = canEdit;
    }
}