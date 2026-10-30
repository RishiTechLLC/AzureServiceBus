# Azure Service Bus samples (.NET 10)

Two console applications that show the full lifecycle of Azure Service Bus messages,
including dead-letter queue (DLQ) handling.

| Project | Purpose |
| --- | --- |
| `src/ServiceBus.Sender` | Posts messages: single, batch, scheduled, sessions, topic, and test messages that trigger failure paths. Can also create the entities. |
| `src/ServiceBus.Receiver` | Receives and settles messages (complete, **release/abandon**, defer, dead-letter), processes sessions and subscriptions, and manages the DLQ. |
| `src/ServiceBus.Shared` | Configuration, client factory (SAS or Microsoft Entra ID), message model, DLQ resubmit logic, admin helpers, console UI. |
| `tests/ServiceBus.Tests` | xUnit tests for the message handling and DLQ resubmit logic. |

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- An Azure Service Bus namespace. **Standard** tier or higher is needed for topics, sessions and duplicate detection.

## Configuration

Settings live in the `ServiceBus` section of `appsettings.json` and can be overridden by
user secrets, environment variables or command-line switches.

**Option A – connection string (SAS)**

```bash
cd src/ServiceBus.Sender
dotnet user-secrets set "ServiceBus:ConnectionString" "Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."
```

Both apps share the same `UserSecretsId`, so setting it once is enough. You can also use an
environment variable: `export ServiceBus__ConnectionString="Endpoint=..."`.

**Option B – Microsoft Entra ID (no secrets)**

```bash
export ServiceBus__FullyQualifiedNamespace="<ns>.servicebus.windows.net"
az login
```

`DefaultAzureCredential` is used. Your identity needs the **Azure Service Bus Data Sender** /
**Data Receiver** roles. Creating entities and reading statistics also need **Owner** or
**Contributor** rights on the namespace.

Other settings:

| Setting | Default | Meaning |
| --- | --- | --- |
| `QueueName` / `SessionQueueName` | `orders` / `orders-sessions` | Standard queue and session-enabled queue |
| `TopicName` / `SubscriptionName` | `order-events` / `all-orders` | Pub/sub entities |
| `UseWebSockets` | `false` | Use AMQP over WebSockets (port 443) when port 5671 is blocked |
| `Processing:MaxConcurrentCalls` | `4` | Processor concurrency |
| `Processing:MaxAutoLockRenewalDuration` | `00:05:00` | Automatic lock renewal for long-running handlers |
| `DeadLetter:RetryableReasons` | `MaxDeliveryCountExceeded`, `TTLExpiredException`, `TransientFailure` | DLQ reasons that are safe to resubmit automatically |
| `DeadLetter:MaxResubmitAttempts` | `3` | Poison protection: a message is never resubmitted more often than this |
| `DeadLetter:ArchiveFolder` | `dlq-archive` | Where messages are archived as JSON before they are removed |
| `Retry:*` | exponential, 5 retries | Client retry policy |

## Running

```bash
# 1. Create the queues, topic and subscription (MaxDeliveryCount=5, dead-lettering on expiry, duplicate detection)
dotnet run --project src/ServiceBus.Sender -- setup

# 2. Interactive menus
dotnet run --project src/ServiceBus.Sender
dotnet run --project src/ServiceBus.Receiver
```

Both apps also accept a command for scripting, for example:

```bash
dotnet run --project src/ServiceBus.Sender -- batch 100
dotnet run --project src/ServiceBus.Sender -- schedule 120
dotnet run --project src/ServiceBus.Receiver -- process
dotnet run --project src/ServiceBus.Receiver -- dlq-auto archive
dotnet run --project src/ServiceBus.Receiver -- dlq-peek --target=subscription
```

### Sender commands

| Menu | Command | What it does |
| --- | --- | --- |
| 1 | `send` | Send one JSON order message |
| 2 | `batch [n]` | Send *n* messages in size-safe batches (`ServiceBusMessageBatch`) |
| 3 | `schedule [sec]` | Schedule a message for later delivery; prints the sequence number |
| 4 | `cancel <seq>` | Cancel a scheduled message |
| 5 | `session [s] [m]` | Send *m* ordered messages to each of *s* sessions |
| 6 | `topic [n]` | Publish to the topic (fan-out to subscriptions) |
| 7 | `transient` | Message the receiver keeps releasing; the broker moves it to the DLQ after `MaxDeliveryCount` |
| 8 | `fatal` | Message the receiver dead-letters immediately (`ValidationFailed`) |
| 9 | `malformed` | Invalid JSON body; dead-lettered as `DeserializationFailed` |
| 10 | `defer` | Message the receiver defers |
| 11 | `ttl [sec]` | Message with a short time-to-live; moved to the DLQ on expiry if nobody receives it |
| 12 | `duplicate` | Same `MessageId` sent twice; duplicate detection keeps one |
| s / c | `stats` / `setup` | Show message counts / create entities |

### Receiver commands

| Menu | Command | What it does |
| --- | --- | --- |
| 1 | `process` | `ServiceBusProcessor` with concurrency, prefetch, lock renewal and explicit settlement |
| 2 | `sessions` | `ServiceBusSessionProcessor`: FIFO per session, progress stored in session state |
| 3 | `receive [n]` | Receive in peek-lock mode and pick **complete**, **release (abandon)**, **defer** or **dead-letter** for each message |
| 4 | `peek [n]` | Browse without locking |
| 5 | `deferred` | Fetch deferred messages by their stored sequence numbers and complete them |
| 6 | `dlq-peek [n]` | Browse the DLQ with reason, description and source, plus a count per reason |
| 7 | `dlq-resubmit [reason]` | Send DLQ messages (all, or only one reason) back to the main entity |
| 8 | `dlq-auto [archive]` | Apply the retry policy: resubmit retryable reasons, then leave or archive the rest |
| 9 | `dlq-listen` | Watch the DLQ continuously: resubmit retryable messages; archive and alert on the rest |
| 10 | `dlq-archive` | Write every DLQ message to JSON, then remove it |
| 11 | `dlq-purge` | Permanently delete every DLQ message |
| e | `--target=subscription` | Switch between the queue and the topic subscription |

Long-running modes stop on **Enter** or **Ctrl+C**. In-flight messages are allowed to finish first.

## How the DLQ handling works

```
Sender ──► orders ──► Receiver ── complete ──► done
                │         ├── release (abandon) ──► redelivered … > MaxDeliveryCount ─┐
                │         ├── defer ──► fetched later by sequence number              │
                │         └── dead-letter (poison / validation) ─────────────────────┤
                └── TTL expired ─────────────────────────────────────────────────────┤
                                                                                      ▼
                                                                         orders/$DeadLetterQueue
                                                       resubmit ◄── policy ──► archive JSON / leave / purge
```

- **Resubmitting** copies the body, properties and correlation ID. It then:
  - moves `DeadLetterReason`/`DeadLetterErrorDescription` into `OriginalDeadLetter*` properties
  - increments `ResubmitCount`
  - sets a new `MessageId` (`<id>-r<n>`) so duplicate detection does not drop it; the original is kept in `OriginalMessageId`
- **Poison protection:** a message that has reached `MaxResubmitAttempts` stays in the DLQ, or is archived. It is never resubmitted again.
- **At-least-once delivery:** a resubmit sends the copy first and only then completes the DLQ message. If the app crashes between those two steps the message can be delivered twice, so handlers should be idempotent.
- **Subscriptions:** resubmitting from a subscription's DLQ sends the message to the topic, so every subscription receives it again.

## Tests

```bash
dotnet test
```
