# CloudFabric.EventSourcing: Offline-First Sync — Консолидированный План v3

> **Документ для LLM-имплементации**
> Этот план создан с учётом полной архитектуры проекта и служит чёткими инструкциями для реализации.

---

## 📋 Резюме Архитектуры Проекта

### Ключевые Абстракции

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ТЕКУЩАЯ АРХИТЕКТУРА                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  CloudFabric.EventSourcing.EventStore (Базовые контракты)           │
│  ├── IEvent (интерфейс события)                                     │
│  ├── Event (record, базовая реализация)                             │
│  ├── IEventStore (интерфейс хранилища)                              │
│  ├── IEventStoreFactory                                             │
│  ├── EventWrapper (сериализация/десериализация)                     │
│  └── Persistence/ (EventUserInfo, StreamInfo)                       │
│                                                                      │
│  CloudFabric.EventSourcing.Domain (DDD слой)                        │
│  ├── AggregateBase (абстрактный агрегат)                            │
│  ├── AggregateRepository<T> (работа с агрегатами)                   │
│  └── AggregateRepositoryFactory                                     │
│                                                                      │
│  CloudFabric.Projections (Read Models)                              │
│  ├── ProjectionDocument (базовый документ)                          │
│  ├── IProjectionRepository<T>                                       │
│  ├── ProjectionRepositoryFactory                                    │
│  ├── ProjectionBuilder<T> (обработчик событий)                      │
│  ├── ProjectionsEngine (координатор)                                │
│  └── EventsObserver (слушатель событий)                             │
│                                                                      │
│  Implementations/ (Реализации для разных хранилищ)                  │
│  ├── CloudFabric.EventSourcing.EventStore.Postgresql/               │
│  │   ├── PostgresqlEventStore : IEventStore                         │
│  │   ├── PostgresqlEventStoreEventObserver : EventsObserver         │
│  │   └── PostgresqlMetadataRepository                               │
│  ├── CloudFabric.EventSourcing.EventStore.CosmosDb/                 │
│  │   ├── CosmosDbEventStore : IEventStore                           │
│  │   └── CosmosDbEventStoreChangeFeedObserver : EventsObserver      │
│  ├── CloudFabric.EventSourcing.EventStore.InMemory/                 │
│  │   ├── InMemoryEventStore : IEventStore                           │
│  │   └── InMemoryEventStoreEventObserver : EventsObserver           │
│  ├── CloudFabric.Projections.Postgresql/                            │
│  ├── CloudFabric.Projections.CosmosDb/                              │
│  ├── CloudFabric.Projections.InMemory/                              │
│  └── CloudFabric.Projections.ElasticSearch/                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Текущий IEvent и Event

```csharp
// CloudFabric.EventSourcing.EventStore/IEvent.cs
public interface IEvent
{
    Guid AggregateId { get; set; }
    DateTime Timestamp { get; init; }
    string PartitionKey { get; set; }
    string AggregateType { get; set; }
}

// CloudFabric.EventSourcing.EventStore/Event.cs
public record Event : IEvent
{
    public Guid AggregateId { get; set; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string PartitionKey { get; set; }
    public string AggregateType { get; set; }
}
```

### Текущий IEventStore

```csharp
public interface IEventStore
{
    Task<EventStream> LoadStreamAsyncOrThrowNotFound(Guid streamId, string partitionKey, ...);
    Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, ...);
    Task<EventStream> LoadStreamAsync(Guid streamId, string partitionKey, int fromVersion, ...);
    Task<List<IEvent>> LoadEventsAsync(string? partitionKey, DateTime? dateFrom, int limit, ...);
    Task<bool> AppendToStreamAsync(EventUserInfo, Guid streamId, int expectedVersion, IEnumerable<IEvent>, ...);
    Task Initialize(...);
    Task<EventStoreStatistics> GetStatistics(...);
    Task DeleteAll(...);
    Task<bool> HardDeleteAsync(Guid streamId, string partitionKey, ...);
}
```

### Ключевые Особенности для Учёта

1. **Множество реализаций EventStore** — PostgreSQL, CosmosDB, InMemory, Filesystem
2. **Множество реализаций Projections** — PostgreSQL, CosmosDB, InMemory, ElasticSearch, OpenSearch, AzureSearch
3. **EventWrapper** используется для сериализации (содержит `EventType` как `AssemblyQualifiedName`)
4. **PartitionKey** — ключевая концепция для multi-tenancy
5. **EventsObserver** — абстрактный класс, разные реализации для разных хранилищ
6. **ProjectionsEngine** работает через EventsObserver

---

## 📐 Архитектурные Принципы

### 1. Storage-Agnostic Design

```
НЕПРАВИЛЬНО:                        ПРАВИЛЬНО:
┌─────────────────┐                 ┌─────────────────┐
│ SyncService     │                 │ ISyncService    │ (интерфейс)
│ ├── PostgreSQL  │                 └────────┬────────┘
│ └── SQL queries │                          │
└─────────────────┘                 ┌────────┴────────┐
                                    │  SyncService    │ (реализация)
                                    │  └── IEventStore│ (абстракция)
                                    └─────────────────┘
```

### 2. Расширение vs Модификация

```
ПРИНЦИП: Расширять интерфейсы, не ломать существующие контракты

IEventStore (существующий)
    ↓
IEventStore (расширенный опциональными методами)
    или
ISyncEventStore : IEventStore (новый интерфейс)
```

### 3. Backward Compatibility

```csharp
// Sync-поля nullable — старые события работают
public record Event : IEvent
{
    // Существующие поля...
    
    // Новые sync-поля (nullable!)
    public Guid? Id { get; set; }
    public long? HlcTimestamp { get; set; }
    public string? ClientId { get; set; }
}
```

---

## 📁 Структура Файлов (Новые и Изменённые)

```
CloudFabric.EventSourcing/
├── CloudFabric.EventSourcing.EventStore/
│   ├── IEvent.cs                               # MODIFY: +Id
│   ├── Event.cs                                # MODIFY: +Id, +HlcTimestamp, +ClientId, +OperationType, +Preconditions
│   ├── IEventStore.cs                          # MODIFY: +sync методы (опционально через extension interface)
│   ├── Persistence/
│   │   └── EventWrapper.cs                     # MODIFY: поддержка новых полей
│   └── Sync/                                   # NEW: папка
│       ├── HybridLogicalClock.cs               # NEW
│       ├── OperationType.cs                    # NEW
│       ├── OperationTypeAttribute.cs           # NEW
│       ├── EventPreconditions.cs               # NEW
│       ├── SyncStatus.cs                       # NEW
│       ├── ISyncEventStore.cs                  # NEW: extension interface
│       ├── ISyncMetadataRepository.cs          # NEW
│       ├── SyncMetadata.cs                     # NEW
│       ├── IConflictResolver.cs                # NEW
│       ├── ConflictResolver.cs                 # NEW
│       ├── ISyncService.cs                     # NEW
│       ├── SyncService.cs                      # NEW
│       ├── SyncDtos.cs                         # NEW
│       ├── IDeadLetterQueue.cs                 # NEW
│       └── DeadLetterQueue.cs                  # NEW
│
├── CloudFabric.EventSourcing.Domain/
│   ├── AggregateBase.cs                        # MODIFY: +snapshot support
│   ├── AggregateRepository.cs                  # MODIFY: +auto-snapshots
│   └── Snapshots/                              # NEW: папка
│       ├── AggregateSnapshot.cs                # NEW
│       ├── ISnapshotRepository.cs              # NEW
│       └── ISnapshotStrategy.cs                # NEW
│
├── CloudFabric.Projections/
│   ├── ProjectionsEngine.cs                    # MODIFY: +HLC-aware rebuild
│   └── EventsObserver.cs                       # MODIFY: +sync events support
│
├── Implementations/
│   ├── CloudFabric.EventSourcing.EventStore.Postgresql/
│   │   ├── PostgresqlEventStore.cs             # MODIFY: +sync methods, +indices
│   │   └── PostgresqlSyncMetadataRepository.cs # NEW
│   ├── CloudFabric.EventSourcing.EventStore.CosmosDb/
│   │   ├── CosmosDbEventStore.cs               # MODIFY: +sync methods
│   │   └── CosmosDbSyncMetadataRepository.cs   # NEW
│   ├── CloudFabric.EventSourcing.EventStore.InMemory/
│   │   ├── InMemoryEventStore.cs               # MODIFY: +sync methods
│   │   └── InMemorySyncMetadataRepository.cs   # NEW
│   └── CloudFabric.Projections.*/
│       └── (no changes needed for sync)
│
├── CloudFabric.EventSourcing.AspNet/
│   ├── CloudFabric.EventSourcing.AspNet.Postgresql/
│   │   └── Extensions/
│   │       └── ServiceCollectionExtensions.cs  # MODIFY: +sync services
│   └── (other providers...)
│
└── CloudFabric.EventSourcing.Tests/
    ├── Sync/                                   # NEW: папка
    │   ├── HybridLogicalClockTests.cs          # NEW
    │   ├── ConflictResolverTests.cs            # NEW
    │   ├── SyncServiceTests.cs                 # NEW
    │   └── MultiClientSyncTests.cs             # NEW
    └── Domain/
        └── Events/
            └── (test events with OperationType) # MODIFY
```

---

## 🛠 Фазы Реализации

### Фаза 0: Подготовка (Критически Важно)

**Цель:** Не сломать существующий код, подготовить фундамент.

#### 0.1 Добавить Id в IEvent и Event

**Файл:** `CloudFabric.EventSourcing.EventStore/IEvent.cs`

```diff
 public interface IEvent
 {
+    /// <summary>
+    /// Уникальный идентификатор события. 
+    /// Генерируется автоматически, необходим для sync.
+    /// </summary>
+    Guid Id { get; set; }
+    
     Guid AggregateId { get; set; }
     DateTime Timestamp { get; init; }
     string PartitionKey { get; set; }
     string AggregateType { get; set; }
 }
```

**Файл:** `CloudFabric.EventSourcing.EventStore/Event.cs`

```diff
 public record Event : IEvent
 {
+    public Guid Id { get; set; } = Guid.NewGuid();
     public Guid AggregateId { get; set; }
     public DateTime Timestamp { get; init; } = DateTime.UtcNow;
     public string PartitionKey { get; set; }
     public string AggregateType { get; set; }
     // ... конструкторы
 }
```

**⚠️ ВАЖНО:** Это breaking change для сериализации! Все существующие события в БД не будут иметь Id.

**Решение:** При десериализации, если Id == Guid.Empty, генерировать детерминированный Id на основе (StreamId, Version):

```csharp
// В EventWrapper.GetEvent()
var e = (IEvent?)EventData.Deserialize(eventType, EventStoreSerializerOptions.Options);
if (e.Id == Guid.Empty && StreamInfo != null)
{
    // Детерминированный Id для legacy событий
    e.Id = GenerateDeterministicGuid(StreamInfo.Id, StreamInfo.Version);
}
```

#### 0.2 Обновить EventWrapper

**Файл:** `CloudFabric.EventSourcing.EventStore/Persistence/EventWrapper.cs`

Добавить сохранение и загрузку Id события.

#### 0.3 Обновить все реализации EventStore

**PostgreSQL:** Добавить колонку `event_id` в таблицу (миграция).
**CosmosDB:** Поле уже есть в JSON.
**InMemory:** Поле уже есть в памяти.

---

### Фаза 1: Hybrid Logical Clock

**Цель:** Глобальное упорядочивание событий без централизованного сервера.

#### 1.1 Создать HybridLogicalClock

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/HybridLogicalClock.cs`

**Спецификация:**
- Thread-safe (используй lock или Interlocked)
- Формат: 48 бит physical time (ms) + 16 бит logical counter
- NodeId хранится отдельно (string)
- Методы: `Now()`, `Update(receivedHlc)`, `Compare(hlc1, hlc2)`, `CompareFull(hlc1, node1, hlc2, node2)`

**Контракт:**
```csharp
public class HybridLogicalClock
{
    public string NodeId { get; }
    public HybridLogicalClock(string nodeId);
    public long Now();
    public long Update(long receivedHlc);
    public static int Compare(long hlc1, long hlc2);
    public static int CompareFull(long hlc1, string nodeId1, long hlc2, string nodeId2);
}
```

#### 1.2 Тесты HLC

**Файл:** `CloudFabric.EventSourcing.Tests/Sync/HybridLogicalClockTests.cs`

**Тест-кейсы:**
1. `Now_ShouldBeMonotonicallyIncreasing` — последовательные вызовы дают возрастающие значения
2. `Update_ShouldAdvanceClock` — при получении большего HLC, часы продвигаются
3. `Update_ShouldNotGoBackwards` — при получении меньшего HLC, часы не откатываются
4. `ThreadSafety_ConcurrentCalls` — параллельные вызовы не ломают монотонность

---

### Фаза 2: Sync-поля в Event

**Цель:** Расширить Event для поддержки sync без breaking changes.

#### 2.1 Создать вспомогательные типы

**Файлы в `CloudFabric.EventSourcing.EventStore/Sync/`:**

**OperationType.cs:**
```csharp
public enum OperationType : byte
{
    Idempotent = 0,   // LWW
    Additive = 1,     // Merge all
    Transactional = 2 // Reject if precondition failed
}
```

**OperationTypeAttribute.cs:**
```csharp
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public class OperationTypeAttribute : Attribute
{
    public OperationType Type { get; }
    public string[]? AffectedFields { get; }
    public OperationTypeAttribute(OperationType type, params string[] affectedFields);
}
```

**EventPreconditions.cs:**
```csharp
public class EventPreconditions
{
    public long? ExpectedVersion { get; set; }
    public Dictionary<string, object?>? ExpectedState { get; set; }
    // Static factory methods
}
```

**SyncStatus.cs:**
```csharp
public enum SyncStatus : byte
{
    Pending = 0,
    Syncing = 1,
    Confirmed = 2,
    Rejected = 3,
    Conflict = 4,
    PermanentlyRejected = 5
}
```

#### 2.2 Расширить Event

**Файл:** `CloudFabric.EventSourcing.EventStore/Event.cs`

```diff
 public record Event : IEvent
 {
     public Guid Id { get; set; } = Guid.NewGuid();
     public Guid AggregateId { get; set; }
     public DateTime Timestamp { get; init; } = DateTime.UtcNow;
     public string PartitionKey { get; set; }
     public string AggregateType { get; set; }
     
+    // Sync-поля (nullable для backward compatibility)
+    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
+    public long? HlcTimestamp { get; set; }
+    
+    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
+    public string? ClientId { get; set; }
+    
+    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
+    public OperationType? OperationType { get; set; }
+    
+    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
+    public EventPreconditions? Preconditions { get; set; }
+    
+    /// <summary>
+    /// Проверяет, является ли событие sync-событием.
+    /// </summary>
+    [JsonIgnore]
+    public bool IsSyncEnabled => HlcTimestamp.HasValue;
 }
```

**⚠️ ВАЖНО:** Не добавляем SyncStatus в Event! Это клиентское состояние.

---

### Фаза 3: SyncMetadata Storage

**Цель:** Хранить метаданные синхронизации отдельно от событий.

#### 3.1 Создать SyncMetadata и интерфейс репозитория

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/SyncMetadata.cs`

**Поля:**
- EventId (Guid, PK)
- AggregateId (Guid)
- PartitionKey (string)
- HlcTimestamp (long)
- ClientId (string)
- OperationType (byte)
- SyncStatus (byte)
- RetryCount (int)
- RejectionReason (string?)
- PreconditionsJson (string?)
- CreatedAt (DateTime)
- UpdatedAt (DateTime)

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/ISyncMetadataRepository.cs`

**Методы:**
```csharp
public interface ISyncMetadataRepository
{
    Task SaveAsync(SyncMetadata metadata, CancellationToken ct = default);
    Task<SyncMetadata?> GetByEventIdAsync(Guid eventId, CancellationToken ct = default);
    Task<List<SyncMetadata>> GetEventsForAggregateAsync(Guid aggregateId, string partitionKey, long afterHlc, int limit, CancellationToken ct = default);
    Task<List<SyncMetadata>> GetEventsAfterHlcAsync(long hlcTimestamp, string? partitionKey, IEnumerable<Guid>? aggregateIds, int limit, CancellationToken ct = default);
    Task UpdateStatusAsync(Guid eventId, SyncStatus status, string? rejectionReason, CancellationToken ct = default);
    Task<bool> IncrementRetryAsync(Guid eventId, int maxRetries, CancellationToken ct = default);
    Task<List<SyncMetadata>> GetDeadLetterEventsAsync(string? partitionKey, int limit, CancellationToken ct = default);
}
```

#### 3.2 Реализации для каждого хранилища

**PostgreSQL:** `Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/PostgresqlSyncMetadataRepository.cs`

SQL схема:
```sql
CREATE TABLE sync_metadata (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    partition_key VARCHAR(255) NOT NULL,
    hlc_timestamp BIGINT NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    operation_type SMALLINT NOT NULL,
    sync_status SMALLINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    rejection_reason TEXT,
    preconditions_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы
CREATE INDEX idx_sync_meta_aggregate_hlc ON sync_metadata(aggregate_id, partition_key, hlc_timestamp);
CREATE INDEX idx_sync_meta_partition_hlc ON sync_metadata(partition_key, hlc_timestamp);
CREATE INDEX idx_sync_meta_status ON sync_metadata(sync_status) WHERE sync_status IN (0, 1, 4, 5);
```

**CosmosDB:** `Implementations/CloudFabric.EventSourcing.EventStore.CosmosDb/CosmosDbSyncMetadataRepository.cs`

**InMemory:** `Implementations/CloudFabric.EventSourcing.EventStore.InMemory/InMemorySyncMetadataRepository.cs`

---

### Фаза 4: Расширение IEventStore

**Цель:** Добавить методы для sync без breaking changes.

#### 4.1 Создать ISyncEventStore

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/ISyncEventStore.cs`

```csharp
/// <summary>
/// Расширение IEventStore для поддержки sync.
/// Реализуется только теми EventStore, которые поддерживают sync.
/// </summary>
public interface ISyncEventStore : IEventStore
{
    /// <summary>
    /// Загрузить события после указанного HLC.
    /// </summary>
    Task<List<IEvent>> LoadEventsAfterHlcAsync(
        long hlcTimestamp,
        string? partitionKey,
        IEnumerable<Guid>? aggregateIds,
        int limit,
        CancellationToken ct = default
    );
    
    /// <summary>
    /// Получить текущее серверное HLC время.
    /// </summary>
    long GetServerHlc();
    
    /// <summary>
    /// Append с поддержкой sync (возвращает результат для каждого события).
    /// </summary>
    Task<List<SyncAppendResult>> AppendWithSyncAsync(
        EventUserInfo eventUserInfo,
        Guid streamId,
        int expectedVersion,
        IEnumerable<IEvent> events,
        CancellationToken ct = default
    );
}

public class SyncAppendResult
{
    public Guid EventId { get; set; }
    public bool Accepted { get; set; }
    public string? RejectionReason { get; set; }
    public Guid? SupersededByEventId { get; set; }
}
```

#### 4.2 Реализовать в PostgreSQL

**Файл:** `Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/PostgresqlEventStore.cs`

Добавить:
1. Приватное поле `HybridLogicalClock _hlc`
2. Реализацию `ISyncEventStore`
3. Дополнительные индексы в `EnsureTableExistsAsync`

#### 4.3 Реализовать в CosmosDB и InMemory

Аналогично PostgreSQL.

---

### Фаза 5: Conflict Resolution

**Цель:** Автоматическое разрешение конфликтов по типу операции.

#### 5.1 Создать ConflictResolver

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/IConflictResolver.cs`

```csharp
public interface IConflictResolver
{
    ConflictResolutionResult Resolve(
        IEvent incoming,
        IReadOnlyList<IEvent> existingConflicting,
        AggregateBase? aggregate = null
    );
}
```

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/ConflictResolver.cs`

**Логика:**
1. **Idempotent:** LWW по HLC. Если incoming.HLC > existing.HLC для событий с пересекающимися AffectedFields → принять incoming.
2. **Additive:** Всегда принять (проверка на дубликаты по EventId).
3. **Transactional:** Проверить Preconditions против текущего состояния агрегата.

---

### Фаза 6: SyncService

**Цель:** Центральный сервис синхронизации (transport-agnostic).

#### 6.1 Создать интерфейс и DTO

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/ISyncService.cs`

```csharp
public interface ISyncService
{
    Task<SyncPushResponse> PushAsync(SyncPushRequest request, CancellationToken ct = default);
    Task<SyncPullResponse> PullAsync(SyncPullRequest request, CancellationToken ct = default);
    Task<SyncAcknowledgeResponse> AcknowledgeAsync(SyncAcknowledgeRequest request, CancellationToken ct = default);
}
```

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/SyncDtos.cs`

Все DTO классы для request/response.

#### 6.2 Реализовать SyncService

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/SyncService.cs`

**Зависимости:**
- `ISyncEventStore`
- `ISyncMetadataRepository`
- `IConflictResolver`
- `HybridLogicalClock`
- `AggregateRepositoryFactory` (для валидации Transactional)

---

### Фаза 7: Snapshots

**Цель:** Оптимизация для больших агрегатов.

#### 7.1 Создать Snapshot инфраструктуру

**Файлы в `CloudFabric.EventSourcing.Domain/Snapshots/`:**

**AggregateSnapshot.cs:**
```csharp
public class AggregateSnapshot
{
    public Guid AggregateId { get; set; }
    public string PartitionKey { get; set; }
    public string AggregateType { get; set; }
    public int Version { get; set; }
    public long HlcTimestamp { get; set; }
    public string StateJson { get; set; }
    public string StateHash { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

**ISnapshotRepository.cs:**
```csharp
public interface ISnapshotRepository
{
    Task SaveAsync(AggregateSnapshot snapshot, CancellationToken ct = default);
    Task<AggregateSnapshot?> GetLatestAsync(Guid aggregateId, string partitionKey, CancellationToken ct = default);
}
```

**ISnapshotStrategy.cs:**
```csharp
public interface ISnapshotStrategy
{
    bool ShouldCreateSnapshot<T>(T aggregate, int eventsSinceLastSnapshot) where T : AggregateBase;
}
```

#### 7.2 Использовать ProjectionRepository для хранения

**Важно:** НЕ создавать отдельные реализации для каждого хранилища! Использовать существующий `IProjectionRepository<AggregateSnapshotDocument>`.

#### 7.3 Модифицировать AggregateBase

Добавить методы:
- `CreateSnapshot(long hlcTimestamp)`
- `RestoreFromSnapshot(AggregateSnapshot snapshot)`
- Конструктор `(AggregateSnapshot snapshot, IEnumerable<IEvent> deltaEvents)`

#### 7.4 Модифицировать AggregateRepository

Добавить опциональную поддержку auto-snapshots.

---

### Фаза 8: Dead Letter Queue

**Цель:** Обработка permanently rejected событий.

#### 8.1 Создать DLQ

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/IDeadLetterQueue.cs`
**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/DeadLetterQueue.cs`

Использует `ISyncMetadataRepository` для хранения.

---

### Фаза 9: Интеграция с ProjectionsEngine

**Цель:** Rebuild проекций с учётом HLC.

#### 9.1 Модифицировать EventsObserver

Добавить метод `ReplayEventsAfterHlcAsync(long hlc, ...)`.

#### 9.2 Модифицировать ProjectionsEngine

Добавить HLC-aware rebuild.

---

### Фаза 10: AspNet Integration

**Цель:** DI-регистрация для всех провайдеров.

#### 10.1 Модифицировать ServiceCollectionExtensions

Для каждого провайдера (PostgreSQL, CosmosDB, InMemory) добавить регистрацию:
- `ISyncEventStore`
- `ISyncMetadataRepository`
- `IConflictResolver`
- `ISyncService`
- `HybridLogicalClock`
- `ISnapshotRepository`
- `IDeadLetterQueue`

---

### Фаза 11: Тесты

**Цель:** Покрыть все сценарии.

#### 11.1 Unit Tests

- `HybridLogicalClockTests`
- `ConflictResolverTests`
- `SyncMetadataTests`

#### 11.2 Integration Tests

- `SyncServiceTests` (для каждого провайдера)
- `MultiClientSyncTests`
- `SnapshotTests`
- `DlqTests`

---

### Фаза 12: Event Notification Service (Real-Time)

**Цель:** Уведомлять клиентов о новых событиях в реальном времени.

#### 12.1 Создать IEventNotificationService

**Файл:** `CloudFabric.EventSourcing.EventStore/Sync/IEventNotificationService.cs`

```csharp
public interface IEventNotificationService
{
    /// <summary>
    /// Уведомить о добавлении события (вызывается из EventStore.AppendToStreamAsync)
    /// </summary>
    Task NotifyEventAddedAsync(IEvent @event, CancellationToken ct = default);
    
    /// <summary>
    /// Подписаться на события (для клиентов через WebSocket/SSE)
    /// </summary>
    IAsyncEnumerable<IEvent> SubscribeAsync(
        string? partitionKey, 
        long? afterHlc, 
        CancellationToken ct = default
    );
}
```

#### 12.2 Реализации

| Хранилище | Механизм | Файл |
|-----------|----------|------|
| PostgreSQL | LISTEN/NOTIFY | `PostgresqlEventNotificationService.cs` |
| CosmosDB | Change Feed (уже есть!) | Использовать `CosmosDbEventStoreChangeFeedObserver` |
| InMemory | In-process events | `InMemoryEventNotificationService.cs` |
| Redis (опционально) | Pub/Sub | `RedisEventNotificationService.cs` |

#### 12.3 Интеграция с существующим Observer Pattern

```csharp
// PostgresqlEventStore.AppendToStreamAsync — добавить вызов
foreach (var e in events)
{
    foreach (var h in _eventAddedEventHandlers)
    {
        await h(e);  // Существующие handlers (ProjectionsEngine)
    }
    
    // НОВОЕ: Notify через IEventNotificationService
    if (_notificationService != null)
    {
        await _notificationService.NotifyEventAddedAsync(e);
    }
}
```

---

### Фаза 13: Расширение ProjectionRepository для Sync

**Цель:** Добавить методы необходимые для эффективной синхронизации проекций.

#### 13.1 Добавить в IProjectionRepository

**Файл:** `CloudFabric.Projections/IProjectionRepository.cs`

```csharp
public interface IProjectionRepository
{
    // ... существующие методы ...
    
    // НОВОЕ: Batch операции для производительности
    Task UpsertBatch(
        IEnumerable<Dictionary<string, object?>> documents,
        string partitionKey,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );
    
    // НОВОЕ: Атомарные patch операции (без race conditions)
    Task Patch(
        Guid id,
        string partitionKey,
        Dictionary<string, PatchOperation> operations,
        DateTime updatedAt,
        CancellationToken cancellationToken = default,
        ProjectionOperationIndexSelector indexSelector = ProjectionOperationIndexSelector.Write
    );
    
    // НОВОЕ: Получить изменения для sync
    Task<ProjectionChangeSet> GetChangesAfter(
        long changeVersion,
        string? partitionKey,
        int limit = 100,
        CancellationToken cancellationToken = default
    );
}
```

#### 13.2 PatchOperation для атомарных обновлений

**Файл:** `CloudFabric.Projections/PatchOperation.cs`

```csharp
public class PatchOperation
{
    public PatchOperationType Type { get; set; }
    public object? Value { get; set; }
}

public enum PatchOperationType
{
    Set,        // field = value
    Increment,  // field = field + value
    Decrement,  // field = field - value
    Append,     // array.push(value)
    Remove      // array.remove(value)
}
```

#### 13.3 ProjectionChangeSet для sync

**Файл:** `CloudFabric.Projections/ProjectionChangeSet.cs`

```csharp
public class ProjectionChangeSet
{
    public long LatestVersion { get; set; }
    public List<ProjectionChange> Changes { get; set; } = new();
    public bool HasMore { get; set; }
}

public class ProjectionChange
{
    public Guid DocumentId { get; set; }
    public ChangeType Type { get; set; }
    public Dictionary<string, object?>? Document { get; set; }
    public long Version { get; set; }
    public DateTime Timestamp { get; set; }
}

public enum ChangeType
{
    Created,
    Updated,
    Deleted
}
```

#### 13.4 Реализации для каждого хранилища

**PostgreSQL:** `UPDATE ... SET field = field + @value` для атомарности
**CosmosDB:** Partial Document Update API
**InMemory:** lock + modify

#### 13.5 Добавить SelectFields в ProjectionQuery

**Файл:** `CloudFabric.Projections/Queries/ProjectionQuery.cs`

```diff
public class ProjectionQuery
{
    public List<Filter> Filters { get; set; }
    public Sorting? Sorting { get; set; }
    public int Limit { get; set; }
    public int Offset { get; set; }
+   
+   /// <summary>
+   /// Выбрать только указанные поля. null = все поля.
+   /// </summary>
+   public List<string>? SelectFields { get; set; }
}
```

---

### Фаза 14: Declarative Projection Schemas

**Цель:** Унифицировать проекции сервер/клиент через декларативные схемы.

#### 14.1 Создать ProjectionSchema

**Файл:** `CloudFabric.Projections/Schema/ProjectionSchema.cs`

```csharp
/// <summary>
/// Полная схема проекции: структура документа + обработчики событий.
/// Может сериализоваться в JSON и использоваться на клиенте.
/// </summary>
public class ProjectionSchema
{
    public string Name { get; set; }
    
    /// <summary>
    /// Схема документа (существующий ProjectionDocumentSchema)
    /// </summary>
    public ProjectionDocumentSchema DocumentSchema { get; set; }
    
    /// <summary>
    /// Обработчики событий: EventTypeName → Handler
    /// </summary>
    public Dictionary<string, EventHandlerSchema> EventHandlers { get; set; } = new();
    
    /// <summary>
    /// Какие агрегаты нужны для enrichment (cross-aggregate data)
    /// </summary>
    public List<string>? RequiredAggregateTypes { get; set; }
    
    /// <summary>
    /// Поля которые клиент может выбирать для partial sync
    /// </summary>
    public List<string>? ClientSelectableFields { get; set; }
}
```

#### 14.2 EventHandlerSchema

**Файл:** `CloudFabric.Projections/Schema/EventHandlerSchema.cs`

```csharp
public class EventHandlerSchema
{
    public EventHandlerOperation Operation { get; set; }
    
    /// <summary>
    /// Выражение для получения ID документа из события.
    /// Примеры: "$.aggregateId", "$.orderId"
    /// </summary>
    public string DocumentIdExpression { get; set; }
    
    /// <summary>
    /// Для Upsert: маппинг полей события → документ.
    /// Key = поле документа, Value = выражение из события.
    /// Примеры: { "Name": "$.orderName", "ItemsCount": "$.items.length" }
    /// </summary>
    public Dictionary<string, string>? FieldMappings { get; set; }
    
    /// <summary>
    /// Для Patch: операции обновления полей.
    /// </summary>
    public Dictionary<string, SchemaPatchOperation>? PatchOperations { get; set; }
    
    /// <summary>
    /// Для PatchMany: query для поиска документов.
    /// Пример: "customerId == $.aggregateId"
    /// </summary>
    public string? QueryExpression { get; set; }
    
    /// <summary>
    /// Enrichment из других агрегатов.
    /// </summary>
    public EnrichmentSchema? Enrichment { get; set; }
    
    /// <summary>
    /// Escape hatch: имя типа кастомного обработчика.
    /// </summary>
    public string? CustomHandlerTypeName { get; set; }
}

public enum EventHandlerOperation
{
    Upsert,     // Создать или полностью заменить
    Patch,      // Частично обновить один документ
    PatchMany,  // Частично обновить много документов по query
    Delete,     // Удалить документ
    Custom      // Кастомный императивный обработчик
}

public class SchemaPatchOperation
{
    public PatchOperationType Type { get; set; }
    public string? ValueExpression { get; set; }  // "$.item.price"
    public object? ConstantValue { get; set; }     // 1
}

public class EnrichmentSchema
{
    public string SourceType { get; set; }           // "Customer"
    public string SourceKeyExpression { get; set; } // "$.customerId"
    public Dictionary<string, string> Mappings { get; set; } = new();
}
```

#### 14.3 EventTypeRegistry

**Файл:** `CloudFabric.EventSourcing.EventStore/EventTypeRegistry.cs`

```csharp
/// <summary>
/// Реестр типов событий: строковое имя ↔ Type.
/// Необходим для декларативных схем где события указаны как строки.
/// </summary>
public class EventTypeRegistry
{
    private readonly Dictionary<string, Type> _nameToType = new();
    private readonly Dictionary<Type, string> _typeToName = new();
    
    public void Register<TEvent>(string name) where TEvent : IEvent
    {
        _nameToType[name] = typeof(TEvent);
        _typeToName[typeof(TEvent)] = name;
    }
    
    public void RegisterFromAssembly(Assembly assembly, Func<Type, string>? nameSelector = null)
    {
        var eventTypes = assembly.GetTypes()
            .Where(t => typeof(IEvent).IsAssignableFrom(t) && !t.IsAbstract);
            
        foreach (var type in eventTypes)
        {
            var name = nameSelector?.Invoke(type) ?? type.Name;
            _nameToType[name] = type;
            _typeToName[type] = name;
        }
    }
    
    public Type? GetType(string name) => _nameToType.GetValueOrDefault(name);
    public string? GetName(Type type) => _typeToName.GetValueOrDefault(type);
}
```

#### 14.4 IExpressionEvaluator

**Файл:** `CloudFabric.Projections/Schema/IExpressionEvaluator.cs`

```csharp
/// <summary>
/// Вычисляет выражения типа "$.aggregateId", "$.items.length"
/// </summary>
public interface IExpressionEvaluator
{
    object? Evaluate(string expression, object source);
}
```

**Реализации:**
- `SimpleExpressionEvaluator` — для простых путей "$.field.subfield"
- `JsonPathExpressionEvaluator` — для полного JSONPath (опционально, через библиотеку)

#### 14.5 SchemaBasedProjectionBuilder

**Файл:** `CloudFabric.Projections/Schema/SchemaBasedProjectionBuilder.cs`

```csharp
/// <summary>
/// ProjectionBuilder который работает по декларативной схеме.
/// </summary>
public class SchemaBasedProjectionBuilder : IProjectionBuilder
{
    private readonly ProjectionSchema _schema;
    private readonly EventTypeRegistry _eventTypeRegistry;
    private readonly IExpressionEvaluator _expressionEvaluator;
    private readonly ProjectionRepositoryFactory _repositoryFactory;
    private readonly AggregateRepositoryFactory? _aggregateRepositoryFactory;
    
    public HashSet<Type> HandledEventTypes { get; }
    
    public SchemaBasedProjectionBuilder(
        ProjectionSchema schema,
        EventTypeRegistry eventTypeRegistry,
        IExpressionEvaluator expressionEvaluator,
        ProjectionRepositoryFactory repositoryFactory,
        AggregateRepositoryFactory? aggregateRepositoryFactory = null)
    {
        _schema = schema;
        _eventTypeRegistry = eventTypeRegistry;
        _expressionEvaluator = expressionEvaluator;
        _repositoryFactory = repositoryFactory;
        _aggregateRepositoryFactory = aggregateRepositoryFactory;
        
        // Заполняем HandledEventTypes из схемы
        HandledEventTypes = schema.EventHandlers.Keys
            .Select(name => eventTypeRegistry.GetType(name))
            .Where(t => t != null)
            .ToHashSet()!;
    }
    
    public async Task ApplyEvent(IEvent @event)
    {
        var eventTypeName = _eventTypeRegistry.GetName(@event.GetType());
        if (eventTypeName == null || !_schema.EventHandlers.TryGetValue(eventTypeName, out var handler))
            return;
            
        await ExecuteHandler(handler, @event);
    }
    
    private async Task ExecuteHandler(EventHandlerSchema handler, IEvent @event)
    {
        switch (handler.Operation)
        {
            case EventHandlerOperation.Upsert:
                await HandleUpsert(handler, @event);
                break;
            case EventHandlerOperation.Patch:
                await HandlePatch(handler, @event);
                break;
            case EventHandlerOperation.PatchMany:
                await HandlePatchMany(handler, @event);
                break;
            case EventHandlerOperation.Delete:
                await HandleDelete(handler, @event);
                break;
            case EventHandlerOperation.Custom:
                await HandleCustom(handler, @event);
                break;
        }
    }
    
    // ... реализации HandleUpsert, HandlePatch и т.д.
}
```

#### 14.6 Узкие Места для Имплементации

| # | Проблема | Решение |
|---|----------|---------|
| 1 | HandledEventTypes через reflection | Заполнять из схемы + EventTypeRegistry |
| 2 | dynamic dispatch на On() | Switch по handler.Operation |
| 3 | JSONPath evaluation | IExpressionEvaluator |
| 4 | Query как строка | Парсер в ProjectionQuery |
| 5 | Cross-aggregate enrichment | AggregateRepositoryFactory (опционально) |

---

### Фаза 15: Client SDK Architecture

**Цель:** Определить архитектуру клиентских SDK.

#### 15.1 Общая Концепция

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SHARED SCHEMA (JSON)                              │
│                                                                          │
│  ProjectionSchema сериализуется в JSON и используется:                  │
│  - На сервере: SchemaBasedProjectionBuilder                             │
│  - На клиенте: LocalProjection                                          │
└─────────────────────────────────────────────────────────────────────────┘
          │                                        │
          ▼                                        ▼
┌─────────────────────┐                ┌─────────────────────┐
│       SERVER        │                │       CLIENT        │
├─────────────────────┤                ├─────────────────────┤
│  SchemaBasedProj-   │                │  LocalProjection    │
│  ectionBuilder      │                │  (in-memory/SQLite) │
│       ↓             │                │       ↓             │
│  IProjection-       │◄──── SYNC ────►│  LocalStorage       │
│  Repository         │   (events +    │  (IndexedDB/SQLite) │
│  (PostgreSQL/etc)   │   snapshots)   │                     │
└─────────────────────┘                └─────────────────────┘
```

#### 15.2 API Клиентского SDK

```typescript
// TypeScript
interface SyncClient {
    // Конфигурация
    configure(options: SyncClientOptions): void;
    
    // Подключение
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    
    // Проекции
    projection<T>(schema: ProjectionSchema): LocalProjection<T>;
    projection<T>(name: string, options?: { fields?: string[] }): LocalProjection<T>;
    
    // События (для сложных случаев)
    subscribeToEvents(eventTypes: string[], handler: (event: IEvent) => void): Subscription;
    
    // Отправка событий (offline-first)
    appendEvent(event: IEvent): Promise<void>;
    
    // Статус синхронизации
    readonly syncStatus: SyncStatus;
    onSyncStatusChange(handler: (status: SyncStatus) => void): Subscription;
}

interface LocalProjection<T> {
    // Query
    query(options?: QueryOptions): Promise<T[]>;
    single(id: string): Promise<T | null>;
    
    // Подписка на изменения (reactive)
    subscribe(options: QueryOptions, handler: (items: T[]) => void): Subscription;
    
    // Для React/Vue/Svelte
    useQuery(options?: QueryOptions): T[];
}
```

#### 15.3 Целевые Платформы

| Платформа | Язык | Local Storage | Приоритет |
|-----------|------|---------------|-----------|
| Web | TypeScript | IndexedDB | P1 |
| iOS/macOS | Swift | SQLite/CoreData | P2 |
| Android | Kotlin | SQLite/Room | P2 |
| .NET MAUI | C# | SQLite | P2 |
| Flutter | Dart | SQLite/Hive | P3 |

---

## 📝 Инструкции для LLM-Имплементации

### Общие Правила

1. **Не писать код напрямую в план.** Давать чёткие спецификации.
2. **Проверять типы.** Все файлы должны компилироваться после каждого изменения.
3. **Backward compatibility.** Существующие тесты должны проходить.
4. **Один файл — одна ответственность.**
5. **Использовать существующие паттерны.** Смотреть как сделаны существующие реализации.

### Контрольные Точки

После каждой фазы:
1. Запустить `dotnet build`
2. Запустить существующие тесты
3. Запустить новые тесты для фазы

### Навигация по Проекту

```
КЛЮЧЕВЫЕ ФАЙЛЫ ДЛЯ ИЗУЧЕНИЯ:

1. Базовые контракты:
   CloudFabric.EventSourcing.EventStore/IEvent.cs
   CloudFabric.EventSourcing.EventStore/Event.cs
   CloudFabric.EventSourcing.EventStore/IEventStore.cs

2. Как делать реализации:
   Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/PostgresqlEventStore.cs
   Implementations/CloudFabric.EventSourcing.EventStore.InMemory/InMemoryEventStore.cs

3. Как работают проекции:
   CloudFabric.Projections/ProjectionsEngine.cs
   CloudFabric.Projections/EventsObserver.cs
   CloudFabric.Projections/ProjectionBuilder.cs

4. Как регистрировать в DI:
   CloudFabric.EventSourcing.AspNet.Postgresql/Extensions/ServiceCollectionExtensions.cs

5. Как писать тесты:
   CloudFabric.EventSourcing.Tests/OrderTests.cs
   CloudFabric.EventSourcing.Tests/Domain/Order.cs
```

### Частые Ошибки (Избегать!)

1. ❌ Привязка к конкретному хранилищу (SQL в абстракциях)
2. ❌ Забыть обновить все реализации (PostgreSQL, CosmosDB, InMemory)
3. ❌ Сломать сериализацию EventWrapper
4. ❌ Не учесть nullable для backward compatibility
5. ❌ Дублировать код вместо использования существующих абстракций
6. ❌ Забыть про индексы в БД

---

## 📊 Checklist для Каждой Фазы

### Фаза X Checklist

- [ ] Создать новые файлы
- [ ] Модифицировать существующие файлы
- [ ] Обновить PostgreSQL реализацию
- [ ] Обновить CosmosDB реализацию
- [ ] Обновить InMemory реализацию
- [ ] Добавить SQL миграции (если нужно)
- [ ] Обновить DI регистрацию
- [ ] Написать unit тесты
- [ ] Написать integration тесты
- [ ] Проверить компиляцию
- [ ] Проверить существующие тесты

---

## 🔗 Зависимости между Фазами

```
Фаза 0 (Id в Event)
    ↓
Фаза 1 (HLC)
    ↓
Фаза 2 (Sync-поля в Event)
    ↓
Фаза 3 (SyncMetadata) ←────────────┐
    ↓                               │
Фаза 4 (ISyncEventStore)           │
    ↓                               │
Фаза 5 (ConflictResolver)          │
    ↓                               │
Фаза 6 (SyncService) ──────────────┤
    ↓                               │
Фаза 7 (Snapshots) ─────────────────┤
    ↓                               │
Фаза 8 (DLQ) ───────────────────────┘
    ↓
Фаза 9 (ProjectionsEngine)
    ↓
Фаза 10 (AspNet)
    ↓
Фаза 11 (Тесты)
    ↓
Фаза 12 (Event Notification) ←── Real-time sync
    ↓
Фаза 13 (ProjectionRepo Extensions) ←── Batch, Patch, ChangeTracking
    ↓
Фаза 14 (Declarative Schemas) ←── Unified Server/Client
    ↓
Фаза 15 (Client SDK) ←── TypeScript, Mobile
```

---

## 📈 Приоритеты

| Приоритет | Фазы | Описание |
|-----------|------|----------|
| P0 (Must) | 0, 1, 2, 3, 4 | Базовая sync инфраструктура |
| P1 (Should) | 5, 6, 12 | Conflict resolution, SyncService, Real-time |
| P2 (Could) | 7, 8, 9, 13 | Оптимизации (snapshots, DLQ, projections, repo extensions) |
| P3 (Won't now) | 14, 15 | Declarative schemas и Client SDK (отдельный этап) |

---

## 🔍 Анализ Узких Мест (Bottlenecks)

### ProjectionBuilder.cs — 7 проблем

| # | Проблема | Текущий код | Решение | Приоритет |
|---|----------|-------------|---------|-----------|
| 1 | HandledEventTypes через reflection | `GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandleEvent<>))` | `EventTypeRegistry` + заполнение из схемы | P1 |
| 2 | Dynamic dispatch | `(this as dynamic).On((dynamic)@event)` | Switch по `EventHandlerSchema.Operation` | P1 |
| 3 | Нет маппинга string→Type | Использует `AssemblyQualifiedName` | `EventTypeRegistry.GetType(name)` | P1 |
| 4 | Нет JSONPath evaluator | Hardcoded property access | `IExpressionEvaluator` | P2 |
| 5 | Нет enrichment | Нельзя подтянуть данные из другого агрегата | `EnrichmentSchema` + `AggregateRepositoryFactory` | P2 |
| 6 | Нет PatchMany | Обновление одного документа | Query → batch update | P2 |
| 7 | UpdateDocument vs full replace | `Upsert` всегда заменяет весь документ | `IProjectionRepository.Patch()` | P1 |

### ProjectionRepository.cs — 5 проблем

| # | Проблема | Текущий код | Решение | Приоритет |
|---|----------|-------------|---------|-----------|
| 1 | Нет batch upsert | Single document operations | `UpsertBatch()` метод | P1 |
| 2 | Нет атомарного patch | Полная замена документа | `Patch()` с `PatchOperation` | P1 |
| 3 | Нет field selection | Query возвращает все поля | `ProjectionQuery.SelectFields` | P2 |
| 4 | Нет change notifications | Polling only | `GetChangesAfter()` + versioning | P1 |
| 5 | Index versioning complexity | `EnsureIndex`/`UpdateIndex` для sync | Sync-specific index strategy | P2 |

### Приоритетная Матрица для Declarative Projections

```
                        СЛОЖНОСТЬ
                    Low         High
                ┌───────────┬───────────┐
           High │    P0     │    P1     │
                │ EventType │ Patch     │
    IMPACT      │ Registry  │ Operations│
                ├───────────┼───────────┤
           Low  │    P2     │    P3     │
                │ SelectFld │ Enrichment│
                │           │ Cross-Agg │
                └───────────┴───────────┘
```

---

**Документ создан:** 29.11.2025
**Версия:** 3.1 (с Declarative Projections и Bottleneck Analysis)
