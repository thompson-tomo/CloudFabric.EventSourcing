[![codecov](https://codecov.io/gh/Tech-Fabric/CloudFabric.EventSourcing/graph/badge.svg?token=NR55NTMBTP)](https://codecov.io/gh/Tech-Fabric/CloudFabric.EventSourcing)

# CloudFabric.EventSourcing

Event Sourcing + CQRS library for .NET with pluggable backends for both event storage and projections (read models). Supports PostgreSQL, ElasticSearch, CosmosDB, and in-memory backends.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [1. Define Events](#1-define-events)
  - [2. Define an Aggregate](#2-define-an-aggregate)
  - [3. Set Up the Event Store](#3-set-up-the-event-store)
  - [4. Save and Load Aggregates](#4-save-and-load-aggregates)
- [Projections (Read Models)](#projections-read-models)
  - [Define a Projection Document](#define-a-projection-document)
  - [Define a Projection Builder](#define-a-projection-builder)
  - [Wire Up the Projections Engine](#wire-up-the-projections-engine)
  - [Query Projections](#query-projections)
- [Projection Rebuild](#projection-rebuild)
- [Batch Mode (Bulk Import)](#batch-mode-bulk-import)
  - [Batch Projection Writes](#batch-projection-writes)
  - [Batch Event Store Insert](#batch-event-store-insert)
  - [Full Import Example](#full-import-example)
  - [Batch Update Existing Aggregates](#batch-update-existing-aggregates)
  - [Progress Tracking](#progress-tracking)
  - [Batch Mode During Rebuild](#batch-mode-during-rebuild)
  - [Thread Safety and Horizontal Scaling](#thread-safety-and-horizontal-scaling)
- [Cross-Aggregate Events](#cross-aggregate-events)
- [Supported Backends](#supported-backends)
- [Development and Testing](#development-and-testing)

## Overview

The library implements the Event Sourcing pattern with CQRS (Command Query Responsibility Segregation):

- **Write side**: Domain aggregates emit events. All state changes are captured as an immutable sequence of events.
- **Read side**: Projection builders listen to events and maintain materialized views (projections) optimized for querying.

Key features:

- DDD-style aggregates with event-driven state changes
- Optimistic concurrency on event streams
- Pluggable backends for event storage and projections
- Automatic projection rebuilds with zero-downtime index switching
- Batch mode for high-throughput import scenarios (1M+ records)
- Cross-aggregate events for bulk domain operations
- Full-text search, filtering, sorting, and pagination on projections

## Getting Started

### 1. Define Events

Events are immutable records that describe what happened in the domain. All events inherit from `Event`:

```csharp
public record OrderPlaced : Event
{
    public OrderPlaced(Guid id, string orderName, string partitionKey, List<OrderItem> items)
    {
        AggregateId = id;
        OrderName = orderName;
        PartitionKey = partitionKey;
        Items = items;
    }

    public string OrderName { get; init; }
    public List<OrderItem> Items { get; init; }
}

public record OrderItemAdded : Event
{
    public OrderItemAdded(Guid id, OrderItem item, string partitionKey)
    {
        AggregateId = id;
        Item = item;
        PartitionKey = partitionKey;
    }

    public OrderItem Item { get; init; }
}
```

### 2. Define an Aggregate

Aggregates inherit from `AggregateBase`. State changes happen only through events:

```csharp
public class Order : AggregateBase
{
    // Constructor for creating a new aggregate
    public Order(Guid id, string orderName, List<OrderItem> items)
        : base(id)
    {
        Apply(new OrderPlaced(id, orderName, PartitionKey, items));
    }

    // Constructor for loading from event history (required)
    public Order(IEnumerable<IEvent> events) : base(events) { }

    // Partition key for multi-tenancy / sharding
    public override string PartitionKey => "orders";

    // State properties
    public string OrderName { get; private set; }
    public List<OrderItem> Items { get; private set; } = new();

    // Business methods raise events via Apply()
    public void AddItem(OrderItem item)
    {
        Apply(new OrderItemAdded(Id, item, PartitionKey));
    }

    // Event handlers — called via dynamic dispatch from Apply()
    // Naming convention: public void On(TEvent @event)
    public void On(OrderPlaced @event)
    {
        OrderName = @event.OrderName;
        Items = new List<OrderItem>(@event.Items);
    }

    public void On(OrderItemAdded @event)
    {
        Items.Add(@event.Item);
    }
}
```

Key points:
- `Apply(event)` adds the event to `UncommittedEvents` and calls the matching `On()` handler via dynamic dispatch.
- The `Order(IEnumerable<IEvent> events)` constructor is used when loading from the event store — it replays all events to reconstruct state.
- `PartitionKey` is required on all aggregates for multi-tenancy support.

### 3. Set Up the Event Store

#### In-Memory (for tests / prototyping)

```csharp
var eventStore = new InMemoryEventStore(
    new ConcurrentDictionary<(Guid, string), List<string>>()
);
await eventStore.Initialize();
```

#### PostgreSQL

```csharp
var eventStore = new PostgresqlEventStore(
    connectionString: "Host=localhost;Port=5432;Database=myapp;Username=myuser;Password=mypass",
    eventsTableName: "events",
    itemsTableName: "event_items"
);
await eventStore.Initialize();
```

### 4. Save and Load Aggregates

Use `AggregateRepository<T>` to persist and load aggregates:

```csharp
var repository = new AggregateRepository<Order>(eventStore);
var userInfo = new EventUserInfo(userId);

// Create and save a new aggregate
var order = new Order(Guid.NewGuid(), "Birthday Gift", new List<OrderItem>());
await repository.SaveAsync(userInfo, order);

// Load an existing aggregate (returns null if not found)
var loaded = await repository.LoadAsync(order.Id, order.PartitionKey);

// Load or throw if not found
var loaded = await repository.LoadAsyncOrThrowNotFound(order.Id, order.PartitionKey);

// Modify and save
loaded.AddItem(new OrderItem(DateTime.UtcNow, "Cake", 25.99m));
await repository.SaveAsync(userInfo, loaded);
```

`SaveAsync` uses optimistic concurrency — if another process has appended events to the same stream since the aggregate was loaded, the save will return `false`.

## Projections (Read Models)

Projections are materialized views optimized for querying. They are built automatically from events.

### Define a Projection Document

Projection documents inherit from `ProjectionDocument`. Use `[ProjectionDocumentProperty]` to configure indexing:

```csharp
[ProjectionDocument]
public class OrderListProjectionItem : ProjectionDocument
{
    [ProjectionDocumentProperty(IsSearchable = true)]
    public string Name { get; set; } = string.Empty;

    [ProjectionDocumentProperty(IsFilterable = true, IsSortable = true)]
    public long ItemsCount { get; set; } = 0;

    [ProjectionDocumentProperty(IsFilterable = true)]
    public decimal TotalAmount { get; set; } = 0;

    [ProjectionDocumentProperty(IsNestedObject = true)]
    public OrderProjectionUserInfo CreatedBy { get; set; }

    [ProjectionDocumentProperty(IsNestedArray = true)]
    public List<OrderProjectionOrderItem> Items { get; set; } = new();
}
```

Attribute options:
- `IsSearchable` — full-text search
- `IsFilterable` — can be used in filter conditions
- `IsSortable` — can be used for ordering results
- `IsFacetable` — faceted search support
- `IsNestedObject` / `IsNestedArray` — nested documents

### Define a Projection Builder

A projection builder listens to events and updates projection documents. Implement `IHandleEvent<TEvent>` for each event type:

```csharp
public class OrdersListProjectionBuilder : ProjectionBuilder<OrderListProjectionItem>,
    IHandleEvent<OrderPlaced>,
    IHandleEvent<OrderItemAdded>
{
    public OrdersListProjectionBuilder(
        ProjectionRepositoryFactory projectionRepositoryFactory,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write)
        : base(projectionRepositoryFactory, indexSelector)
    {
    }

    public async Task On(OrderPlaced @event)
    {
        await UpsertDocument(
            new OrderListProjectionItem
            {
                Id = @event.AggregateId,
                Name = @event.OrderName,
                ItemsCount = @event.Items.Count,
                TotalAmount = @event.Items.Sum(i => i.Amount)
            },
            @event.PartitionKey,
            @event.Timestamp
        );
    }

    public async Task On(OrderItemAdded @event)
    {
        await UpdateDocument(
            @event.AggregateId,
            @event.PartitionKey,
            @event.Timestamp,
            order =>
            {
                order.Items.Add(new OrderProjectionOrderItem
                {
                    Name = @event.Item.Name,
                    Amount = @event.Item.Amount
                });
                order.ItemsCount++;
                order.TotalAmount += @event.Item.Amount;
            }
        );
    }
}
```

Available methods in `ProjectionBuilder<T>`:
- `UpsertDocument(document, partitionKey, updatedAt)` — insert or replace a projection document
- `UpdateDocument(id, partitionKey, updatedAt, callback)` — load a document, apply changes, and save
- `DeleteDocument(id, partitionKey)` — remove a projection document
- `UpdateByQuery(query, partitionKey, propertyUpdates, updatedAt)` — server-side bulk update

### Wire Up the Projections Engine

The projections engine connects event observers to projection builders:

```csharp
// 1. Create the event store and projection repository factory
var eventStore = new PostgresqlEventStore(connectionString, "events", "event_items");
await eventStore.Initialize();

var projectionRepositoryFactory = new PostgresqlProjectionRepositoryFactory(
    loggerFactory, projectionsConnectionString
);

// 2. Create an event observer (bridges event store → projection engine)
var eventsObserver = new PostgresqlEventStoreEventObserver(eventStore, logger);

// 3. Build the projections engine using the fluent builder
var projectionsEngine = new ProjectionsEngineBuilder()
    .WithEventsObserver(eventsObserver)
    .AddProjectionBuilder(new OrdersListProjectionBuilder(projectionRepositoryFactory))
    .WithLogger(logger)               // optional
    .WithErrorHandler(errorHandler)    // optional
    .Build();

// 4. Ensure projection indices exist
var repo = projectionRepositoryFactory.GetProjectionRepository<OrderListProjectionItem>();
await repo.EnsureIndex();

// 5. Start listening for events
await projectionsEngine.StartAsync("my-instance");

// ... application runs, events are processed automatically ...

// 6. Stop on shutdown
await projectionsEngine.StopAsync();
```

`ProjectionsEngineBuilder` provides a fluent API for constructing the engine. You can chain multiple `AddProjectionBuilder()` calls to register several projection builders at once.

### Query Projections

Use `ProjectionQuery` to search, filter, sort, and paginate projection data:

```csharp
var repo = projectionRepositoryFactory.GetProjectionRepository<OrderListProjectionItem>();

// Get a single document by ID
var order = await repo.Single(orderId, partitionKey);

// Query with filters, sorting, and pagination
var results = await repo.Query(new ProjectionQuery
{
    Filters = new List<Filter>
    {
        new Filter("ItemsCount", FilterOperator.Greater, 0),
        new Filter("Name", FilterOperator.Contains, "birthday")
    },
    OrderBy = { new SortInfo { KeyPath = "TotalAmount", Order = "desc" } },
    Limit = 20,
    Offset = 0
});

// results.Records — list of matching documents
// results.TotalRecordsFound — total count (for pagination)
```

**Filter operators:**

| Operator | Constant | Description |
|----------|----------|-------------|
| `eq` | `FilterOperator.Equal` | Equals |
| `ne` | `FilterOperator.NotEqual` | Not equals |
| `gt` | `FilterOperator.Greater` | Greater than |
| `ge` | `FilterOperator.GreaterOrEqual` | Greater or equal |
| `lt` | `FilterOperator.Lower` | Less than |
| `le` | `FilterOperator.LowerOrEqual` | Less or equal |
| `string-contains` | `FilterOperator.Contains` | String contains |
| `string-starts-with` | `FilterOperator.StartsWith` | String starts with |
| `string-ends-with` | `FilterOperator.EndsWith` | String ends with |
| `array-contains` | `FilterOperator.ArrayContains` | Array contains value |

Case-insensitive variants: `ContainsIgnoreCase`, `StartsWithIgnoreCase`, `EndsWithIgnoreCase`.

**Combining filters with AND/OR:**

```csharp
var filter = new Filter("Status", FilterOperator.Equal, "Active")
    .And("ItemsCount", FilterOperator.Greater, 0)
    .Or("Priority", FilterOperator.Equal, "High");
```

**Full-text search:**

```csharp
var results = await repo.Query(new ProjectionQuery
{
    SearchText = "birthday cake",  // searches across all IsSearchable fields
    Limit = 10
});
```

## Projection Rebuild

When projection schema changes or projections get corrupted, you can rebuild them from the event history. The library provides zero-downtime rebuilds with automatic index switching.

```csharp
var rebuildProcessor = new ProjectionsRebuildProcessor(
    projectionRepositoryFactory.GetProjectionsIndexStateRepository(),
    async (string connectionId) =>
    {
        var engine = new ProjectionsEngine(eventsObserver);
        engine.AddProjectionBuilder(
            new OrdersListProjectionBuilder(
                projectionRepositoryFactory,
                ProjectionOperationIndexSelector.ProjectionRebuild
            )
        );
        return engine;
    },
    logger,
    projectionRepositoryFactory  // optional: enables batch mode during rebuild
);

// Rebuilds all projections that need rebuilding
await rebuildProcessor.RebuildProjectionsThatRequireRebuild();
```

When a `ProjectionRepositoryFactory` is passed in, the rebuild processor automatically enables batch mode — buffering upserts and flushing in bulk after each chunk of 250 events.

## Batch Mode (Bulk Import)

For high-throughput scenarios (importing 100K+ records), batch mode buffers `Upsert` and `Delete` calls in memory and writes them as a single bulk operation on flush.

### Batch Projection Writes

```csharp
var factory = GetProjectionRepositoryFactory();
var repo = factory.GetProjectionRepository<OrderListProjectionItem>();

// Enable batch mode on all repositories managed by the factory
factory.BeginBatchOnAll();
try
{
    // All Upsert/Delete calls are now buffered instead of writing immediately
    for (int i = 0; i < 10000; i++)
    {
        await repo.Upsert(documents[i], partitionKey, DateTime.UtcNow);

        // Flush every 500 documents
        if (i % 500 == 0)
            await factory.FlushBatchOnAllAsync();
    }

    // Final flush for remaining buffered items
    await factory.FlushBatchOnAllAsync();
}
finally
{
    factory.EndBatchOnAll();  // exits batch mode, discards any unflushed data
}
```

How it works under the hood:
- **PostgreSQL**: `FlushBatchAsync()` executes a single multi-row `INSERT ... ON CONFLICT DO UPDATE` statement
- **ElasticSearch**: `FlushBatchAsync()` executes a single `_bulk` API request
- **InMemory**: sequential writes (already fast)

Batch mode is transparent to projection builders — `UpsertDocument()`, `UpdateDocument()`, etc. work exactly the same. `Single()` checks the buffer first (read-your-writes), `Query()` and `UpdateByQuery()` auto-flush before executing.

### Batch Event Store Insert

For importing many new aggregates, `SaveMultipleNewAsync` inserts all events in a single database transaction:

```csharp
var repository = new AggregateRepository<Product>(eventStore);
var userInfo = new EventUserInfo(importUserId);

var aggregates = excelRows.Select(row =>
    new Product(Guid.NewGuid(), row.Name, row.Price, row.Category)
).ToList();

// Single transaction: all events for all aggregates
await repository.SaveMultipleNewAsync(userInfo, aggregates);
```

On PostgreSQL this uses `NpgsqlBatch` with sub-batches of 5000 commands — inserting 1M events takes seconds instead of hours.

**Note:** all aggregates must be new (version 0). For updating existing aggregates in bulk, use `SaveAsync` in a loop with batch projection mode enabled.

### Full Import Example

Combining both — batch event store insert + batch projection writes:

```csharp
var factory = GetProjectionRepositoryFactory();
var repository = new AggregateRepository<Product>(eventStore);
var userInfo = new EventUserInfo(importUserId);

// 1. Enable batch mode on projections
factory.BeginBatchOnAll();
try
{
    // 2. Create aggregates from import data
    var aggregates = excelRows.Select(row =>
        new Product(Guid.NewGuid(), row.Name, row.Price, row.Category)
    ).ToList();

    // 3. Batch insert events (single transaction)
    //    Event handlers fire → projection upserts go into the buffer
    await repository.SaveMultipleNewAsync(userInfo, aggregates);

    // 4. Flush projections (single bulk write)
    await factory.FlushBatchOnAllAsync();
}
finally
{
    factory.EndBatchOnAll();
}
```

### Batch Update Existing Aggregates

For mass updates (e.g., updating prices from a new Excel file), use `AppendEventsToMultipleAsync` — it batch-appends events to existing streams without loading aggregates. Stream versions are loaded automatically inside a single transaction:

```csharp
// Parse Excel → for each row, compare with current projection to detect changes
var updates = new List<(Guid, string, IReadOnlyList<IEvent>)>();

foreach (var row in excelRows)
{
    var current = await projectionRepo.Single(row.AggregateId, row.PartitionKey);
    if (current == null) continue;

    var events = new List<IEvent>();
    if (current.Price != row.NewPrice)
        events.Add(new PriceUpdated(row.AggregateId, row.NewPrice, row.PartitionKey));
    if (current.Category != row.NewCategory)
        events.Add(new CategoryUpdated(row.AggregateId, row.NewCategory, row.PartitionKey));

    if (events.Count > 0)
        updates.Add((row.AggregateId, row.PartitionKey, (IReadOnlyList<IEvent>)events));
}

// Batch append + flush in chunks
factory.BeginBatchOnAll();
try
{
    foreach (var chunk in updates.Chunk(1000))
    {
        await repository.AppendEventsToMultipleAsync(userInfo, chunk.ToList());
        await factory.FlushBatchOnAllAsync();
    }
}
finally
{
    factory.EndBatchOnAll();
}
```

Key points:
- Events must contain **absolute values** (e.g., `SetPrice(100)`, not `IncrementPrice(+10)`) since aggregates are not loaded.
- `Single()` in batch mode reads from the buffer (read-your-writes), so changes from previous chunks are visible.
- If a concurrent modification is detected, the transaction fails with `InvalidOperationException`.

### Progress Tracking

For long-running batch operations, use `BatchOperationTracker` to persist progress to the metadata repository. Frontend can poll for updates. Supports multi-tenancy via optional `partitionKey` parameter:

```csharp
// With default partition key:
var tracker = await BatchOperationTracker.StartAsync(
    metadataRepository, "product-update", totalItems: excelRows.Count);

// With tenant-specific partition key:
var tracker = await BatchOperationTracker.StartAsync(
    metadataRepository, "product-update", totalItems: excelRows.Count,
    partitionKey: tenantId);

factory.BeginBatchOnAll();
try
{
    int processed = 0;
    foreach (var chunk in excelRows.Chunk(1000))
    {
        // ... process chunk ...
        processed += chunk.Length;
        await tracker.UpdateProgressAsync(processed);
    }
    await tracker.CompleteAsync();
}
catch (Exception ex)
{
    await tracker.FailAsync(ex.Message);
    throw;
}
finally
{
    factory.EndBatchOnAll();
}

// Frontend polling:
var state = await BatchOperationTracker.LoadAsync(metadataRepository, tracker.State.OperationId);
// state.ProcessedItems, state.TotalItems, state.Status, state.Metadata
```

`BatchOperationState` also has a `Metadata` dictionary for storing operation-specific data (e.g. index info during projection rebuilds).

### Projection Rebuild Progress Tracking

When `IMetadataRepository` is passed to `ProjectionsRebuildProcessor`, it automatically tracks rebuild progress via `BatchOperationTracker`. Each projection rebuild creates a separate operation with type `"projection-rebuild"` and metadata containing index details:

```csharp
var rebuildProcessor = new ProjectionsRebuildProcessor(
    projectionRepositoryFactory.GetProjectionsIndexStateRepository(),
    projectionsEngineFactory,
    logger,
    projectionRepositoryFactory,   // ← enables batch mode during rebuild
    metadataRepository             // ← enables progress tracking
);
```

The rebuild metadata includes:
```json
{
  "projectionName": "OrderListProjection",
  "connectionId": "default",
  "indexNameBeingRebuilt": "order_list_v2",
  "indices": [
    {
      "indexName": "order_list_v1",
      "schemaHash": "abc123",
      "rebuildCompletedAt": "2026-01-15T10:00:00Z",
      "isBeingRebuilt": false
    },
    {
      "indexName": "order_list_v2",
      "schemaHash": "def456",
      "rebuildEventsProcessed": 50000,
      "totalEventsToProcess": 100000,
      "isBeingRebuilt": true
    }
  ]
}
```

Frontend can poll this via `BatchOperationTracker.LoadAsync(metadataRepo, operationId)` to display rebuild progress with detailed index status.

### Thread Safety and Horizontal Scaling

- The batch buffer is protected by `lock` — import and live event processing can run in parallel on the same process.
- While batch mode is active, live events are also buffered — projection latency increases from "immediate" to "until next flush". For a 1M-row import this is a temporary, acceptable trade-off.
- Batch mode is per-instance. In a horizontally scaled deployment (e.g. 50 service instances), one instance can import in batch mode while the other 49 continue normal operation with no impact.

## Cross-Aggregate Events

Cross-aggregate events apply a change to all aggregates of a given type within a partition — useful for bulk operations like "change tag on all orders" or "update pricing tier for all products".

`TargetPartitionKey` is **required** — cross-aggregate events are always scoped to a specific partition (tenant) to ensure data isolation.

### Define a cross-aggregate event

```csharp
public record BulkOrderTagChanged : CrossAggregateEvent
{
    public BulkOrderTagChanged(string aggregateType, string newTag, string targetPartitionKey)
    {
        AggregateType = aggregateType;
        NewTag = newTag;
        TargetPartitionKey = targetPartitionKey;
    }

    public string NewTag { get; init; }
}
```

### Append a cross-aggregate event

```csharp
var evt = new BulkOrderTagChanged(
    aggregateType: typeof(Order).AssemblyQualifiedName!,
    newTag: "priority",
    targetPartitionKey: tenantPartitionKey  // required — scoped to a specific partition
);

await eventStore.AppendGlobalEventAsync(userInfo, evt);
```

### Handle in the aggregate

No changes needed — `AggregateRepository.LoadAsync` automatically merges global events with the aggregate's own events (ordered by timestamp). Add an `On()` handler:

```csharp
public class Order : AggregateBase
{
    public string Tag { get; private set; }

    public void On(BulkOrderTagChanged @event)
    {
        Tag = @event.NewTag;
    }
}
```

### Handle in the projection builder

Implement `IHandleCrossAggregateEvent<T>` and use `OnBulk` for server-side bulk updates:

```csharp
public class OrdersListProjectionBuilder : ProjectionBuilder<OrderListProjectionItem>,
    IHandleCrossAggregateEvent<BulkOrderTagChanged>
{
    public async Task OnBulk(BulkOrderTagChanged @event)
    {
        await UpdateByQuery(
            new ProjectionQuery(),  // empty query = all documents
            @event.PartitionKey,
            new Dictionary<string, object?> { { "Tag", @event.NewTag } },
            @event.Timestamp
        );
    }
}
```

`UpdateByQuery` executes server-side — documents are not loaded into memory. PostgreSQL uses `UPDATE ... SET ... WHERE ...`, ElasticSearch uses `_update_by_query`.

## Supported Backends

### Event Store

| Backend | Package | Notes |
|---------|---------|-------|
| **PostgreSQL** | `CloudFabric.EventSourcing.EventStore.Postgresql` | Full support including batch insert. In-process event observer. |
| **InMemory** | `CloudFabric.EventSourcing.EventStore.InMemory` | For tests and prototyping. No external dependencies. |
| **CosmosDB** | `CloudFabric.EventSourcing.EventStore.CosmosDb` | Change feed observer for distributed scenarios. |
| **Filesystem** | `CloudFabric.EventSourcing.EventStore.Filesystem` | File-based storage. |

### Projection Repository

| Backend | Package | Notes |
|---------|---------|-------|
| **PostgreSQL** | `CloudFabric.Projections.Postgresql` | Bulk flush via multi-row `INSERT ON CONFLICT`. |
| **ElasticSearch** | `CloudFabric.Projections.ElasticSearch` | Bulk flush via `_bulk` API. Full-text search. |
| **InMemory** | `CloudFabric.Projections.InMemory` | For tests. |
| **CosmosDB** | `CloudFabric.Projections.CosmosDb` | Azure Cosmos SDK. |
| **OpenSearch** | `CloudFabric.Projections.OpenSearch` | AWS OpenSearch. |
| **Azure Search** | `CloudFabric.Projections.AzureSearch` | Azure Cognitive Search. |

You can mix backends — for example, use PostgreSQL for event storage and ElasticSearch for projections.

## Development and Testing

### Build

```bash
dotnet build CloudFabric.EventSourcing.sln
```

### Run Tests

```bash
# In-memory tests (no dependencies)
dotnet test Implementations/CloudFabric.EventSourcing.Tests.InMemory

# PostgreSQL tests (requires PostgreSQL on port 5433)
dotnet test Implementations/CloudFabric.EventSourcing.Tests.Postgresql

# ElasticSearch tests (requires PostgreSQL on port 5433 + Elasticsearch on port 9222)
dotnet test Implementations/CloudFabric.EventSourcing.Tests.ElasticSearch

# Run a single test
dotnet test --filter "FullyQualifiedName~OrderTests.TestPlaceOrder"
```

### Infrastructure for Tests

PostgreSQL and Elasticsearch tests require running services. Start them with:

```bash
docker-compose -f docker-compose.tests.yml up -d
```

Non-standard ports are used to avoid conflicts: PostgreSQL on **5433**, Elasticsearch on **9222**.

### Test Structure

All test logic lives in shared base classes in `CloudFabric.EventSourcing.Tests/`. Backend-specific test projects (e.g. `Tests.InMemory`, `Tests.Postgresql`) inherit and only override factory methods — ensuring identical test coverage across all backends.

```csharp
[TestClass]
public class OrderTestsPostgresql : OrderTests
{
    protected override async Task<IEventStore> GetEventStore() { /* PostgreSQL setup */ }
    protected override ProjectionRepositoryFactory GetProjectionRepositoryFactory() { /* ... */ }
    protected override EventsObserver GetEventStoreEventsObserver() { /* ... */ }
}
```
