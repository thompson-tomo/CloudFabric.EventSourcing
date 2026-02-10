using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;

using ToDoList.Domain.Events.UserAccounts;

namespace ToDoList.Domain;

public class UserAccount : AggregateBase
{
    public string FirstName { get; protected set; }

    public string HashedPassword { get; protected set; }

    public string PasswordUpdatedAt { get; protected set; }
    
    public override string PartitionKey => Id.ToString();

    public UserAccount(IEnumerable<IEvent> events) : base(events)
    {
    }

    public UserAccount(Guid id, string firstName, string hashedPassword)
        : base(id)
    {
        Apply(new UserAccountRegistered(id, firstName, hashedPassword));
    }

    public void UpdatePassword(string newHashedPassword)
    {
        Apply(new UserAccountPasswordUpdated(newHashedPassword));
    }

    #region Event Handlers

    public void On(UserAccountRegistered @event)
    {
        FirstName = @event.FirstName;
        HashedPassword = @event.HashedPassword;
    }

    public void On(UserAccountPasswordUpdated @event)
    {
        HashedPassword = @event.NewHashedPassword;
    }

    #endregion
}