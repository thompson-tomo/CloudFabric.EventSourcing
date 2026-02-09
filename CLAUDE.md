# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Build
dotnet build CloudFabric.EventSourcing.sln

# Run all tests
dotnet test

# Run tests for a specific backend
dotnet test Implementations/CloudFabric.EventSourcing.Tests.InMemory    # no dependencies
dotnet test Implementations/CloudFabric.EventSourcing.Tests.Postgresql  # requires PostgreSQL
dotnet test Implementations/CloudFabric.EventSourcing.Tests.ElasticSearch  # requires PostgreSQL + Elasticsearch

# Run a single test
dotnet test --filter "FullyQualifiedName~OrderTests.TestPlaceOrder"

# Run benchmarks
dotnet run --project CloudFabric.EventSourcing.Benchmarks -c Release
```

### Infrastructure for tests

PostgreSQL and Elasticsearch tests require running services. Start them with:

```bash
docker-compose -f docker-compose.tests.yml up -d
```

Non-standard ports are used to avoid conflicts: PostgreSQL on **5433**, Elasticsearch on **9222**.
Configuration is read from environment variables (see `.env.example`), with defaults matching docker-compose.

## Architecture

Event Sourcing + CQRS library with pluggable backends for both event storage and projections (read models).

### Core layers

- **EventStore** (`CloudFabric.EventSourcing.EventStore/`) — `IEventStore`, `IEvent`, `Event`, `EventStream`. Defines the contract for persisting and loading event streams. Optimistic concurrency via expected version on append.
- **Domain** (`CloudFabric.EventSourcing.Domain/`) — `AggregateBase`. Base class for aggregates: raises events via `RaiseEvent()`, applies them via dynamic dispatch to `On(TEvent)` methods. State is reconstructed entirely from events.
- **Projections** (`CloudFabric.Projections/`) — Read-side materialized views. `ProjectionBuilder<T>` consumes events (implements `IHandleEvent<TEvent>`) and updates `IProjectionRepository<T>`. `ProjectionsEngine` coordinates builders. `EventsObserver` bridges event store to engine.

### Backend implementations (`Implementations/`)

Each backend implements `IEventStore`, `IProjectionRepository`, and `EventsObserver`:
- **InMemory** — dictionary-based, no external dependencies
- **Postgresql** — Npgsql, direct SQL
- **CosmosDb** — Azure Cosmos SDK, change feed for observation
- **ElasticSearch/OpenSearch/AzureSearch** — projection repositories only (no event store)
- **Filesystem** — file-based event store

### Test structure

Shared test base class `TestsBaseWithProjections<TProjectionDocument, TProjectionBuilder>` in `CloudFabric.EventSourcing.Tests/` defines all test logic. Implementation-specific test projects (e.g. `Tests.InMemory`, `Tests.Postgresql`) inherit from it and only override factory methods (`GetEventStore()`, `GetProjectionRepositoryFactory()`, `GetEventStoreEventsObserver()`). This ensures identical test coverage across backends.

Test framework: **MSTest** (`[TestClass]`, `[TestMethod]`).

### Key patterns

- **Dynamic dispatch** for event handling: both aggregates (`AggregateBase.RaiseEvent`) and projection builders (`ProjectionBuilder.ApplyEvent`) use `((dynamic)this).On((dynamic)@event)` to route events to typed `On(TEvent)` methods.
- **Partition keys** on all events and aggregates for multi-tenancy/sharding.
- **ProjectionRepositoryFactory** caches repository instances by document type.
- **ProjectionDocumentAttribute / ProjectionDocumentProperty** attributes define projection schema (searchable, filterable, sortable fields).

## Code style

- .NET 9.0, C# with file-scoped namespaces, nullable reference types enabled
- Detailed rules in `.editorconfig`
