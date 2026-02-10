using ToDoList.Domain.Events.UserAccounts;
using CloudFabric.Projections;

namespace ToDoList.Domain.Projections.UserAccounts;


public class UserAccountsProjectionBuilder : ProjectionBuilder<UserAccountsProjectionItem>,
    IHandleEvent<UserAccountRegistered>,
    IHandleEvent<UserAccountEmailAddressChanged>,
    IHandleEvent<UserAccountEmailAddressConfirmed>,
    IHandleEvent<UserAccountEmailAssigned>
{
    public UserAccountsProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector) : base(projectionRepositoryFactory, indexSelector)
    {
    }

    public async System.Threading.Tasks.Task On(UserAccountRegistered evt)
    {
        await UpsertDocument(
            new UserAccountsProjectionItem()
            {
                Id = evt.AggregateId,
                FirstName = evt.FirstName,
            },
            evt.PartitionKey,
            evt.Timestamp
        );
    }

    public async System.Threading.Tasks.Task On(UserAccountEmailAddressChanged evt)
    {
        await UpdateDocument(
            evt.UserAccountId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) =>
            {
                projectionDocument.EmailAddress = evt.NewEmail;
            }
        );
    }

    public async System.Threading.Tasks.Task On(UserAccountEmailAddressConfirmed evt)
    {
        await UpdateDocument(
            evt.UserAccountId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) =>
            {
                projectionDocument.EmailConfirmedAt = DateTime.UtcNow;
            }
        );
    }

    public async System.Threading.Tasks.Task On(UserAccountEmailAssigned evt)
    {
        await UpdateDocument(
            evt.UserAccountId,
            evt.PartitionKey,
            evt.Timestamp,
            (projectionDocument) =>
            {
                projectionDocument.EmailAddress = evt.EmailAddress;
            }
        );
    }
    
    public async System.Threading.Tasks.Task On(AggregateUpdatedEvent<UserAccount> @event)
    {
        await SetDocumentUpdatedAt(@event.AggregateId, @event.PartitionKey, @event.UpdatedAt);
    }
}