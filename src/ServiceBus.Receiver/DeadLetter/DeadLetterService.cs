using System.Text.Json;
using Azure.Messaging.ServiceBus;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Receiver.DeadLetter;

/// <summary>
/// Dead-letter queue (DLQ) tooling: resubmit, policy-based auto-repair, archive, purge and a
/// continuous DLQ listener.
/// </summary>
/// <remarks>
/// Resubmission sends a copy to the main entity and then completes the DLQ message. If the
/// process dies between the two steps the message may be resubmitted twice, so handlers
/// should be idempotent (at-least-once delivery).
/// For a subscription, resubmitted messages go to the topic and are therefore delivered to
/// every subscription of that topic.
/// </remarks>
public sealed class DeadLetterService(ServiceBusClient client, ServiceBusSettings settings)
{
    private static readonly JsonSerializerOptions ArchiveJson = new() { WriteIndented = true };

    private DeadLetterSettings Options => settings.DeadLetter;

    /// <summary>Resubmits every DLQ message (optionally only those with a given reason) to the main entity.</summary>
    public Task<DlqSummary> ResubmitAsync(ServiceBusEntity entity, string? reasonFilter, CancellationToken cancellationToken) =>
        ProcessDeadLettersAsync(entity, retryableReasons: null, reasonFilter, archiveRejected: false, cancellationToken);

    /// <summary>
    /// Applies the configured policy: retryable reasons are resubmitted (up to MaxResubmitAttempts),
    /// everything else is left in the DLQ for an operator, or archived and removed when requested.
    /// </summary>
    public Task<DlqSummary> AutoProcessAsync(ServiceBusEntity entity, bool archiveRejected, CancellationToken cancellationToken) =>
        ProcessDeadLettersAsync(entity, Options.RetryableReasons, reasonFilter: null, archiveRejected, cancellationToken);

    /// <summary>Writes every DLQ message to a JSON file and removes it from the DLQ.</summary>
    public async Task<int> ArchiveAndPurgeAsync(ServiceBusEntity entity, CancellationToken cancellationToken)
    {
        await using var receiver = CreateDlqReceiver(entity, ServiceBusReceiveMode.PeekLock);
        var archived = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await receiver.ReceiveMessagesAsync(Options.BatchSize, TimeSpan.FromSeconds(3), cancellationToken);
            if (messages.Count == 0)
            {
                break;
            }

            foreach (var message in messages)
            {
                await ArchiveAsync(entity, message, cancellationToken);
                await receiver.CompleteMessageAsync(message, cancellationToken);
                archived++;
            }
        }

        ConsoleUi.Success($"Archived and removed {archived} message(s) to '{ArchiveDirectory(entity)}'.");
        return archived;
    }

    /// <summary>Permanently deletes every message from the DLQ.</summary>
    public async Task<int> PurgeAsync(ServiceBusEntity entity, CancellationToken cancellationToken)
    {
        // ReceiveAndDelete removes messages as they are received - fastest way to empty a queue.
        await using var receiver = CreateDlqReceiver(entity, ServiceBusReceiveMode.ReceiveAndDelete);
        var purged = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await receiver.ReceiveMessagesAsync(Options.BatchSize, TimeSpan.FromSeconds(3), cancellationToken);
            if (messages.Count == 0)
            {
                break;
            }

            purged += messages.Count;
            ConsoleUi.Info($"Purged {purged} so far...");
        }

        ConsoleUi.Success($"Purged {purged} message(s) from the dead-letter queue of '{entity}'.");
        return purged;
    }

    /// <summary>
    /// Continuously listens on the DLQ and applies the policy as messages arrive: retryable messages are
    /// resubmitted; others are archived to disk and removed so the DLQ does not grow unbounded.
    /// </summary>
    public async Task ListenAsync(ServiceBusEntity entity, CancellationToken stopToken)
    {
        await using var sender = client.CreateSender(entity.SendPath);
        await using var processor = entity.CreateProcessor(client, new ServiceBusProcessorOptions
        {
            SubQueue = SubQueue.DeadLetter,
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1,
        });

        int resubmitted = 0, archived = 0;

        processor.ProcessMessageAsync += async args =>
        {
            var message = args.Message;
            var decision = DeadLetterResubmitter.Evaluate(message.DeadLetterReason, message.ApplicationProperties, Options.MaxResubmitAttempts, Options.RetryableReasons);

            if (decision == ResubmitDecision.Resubmit)
            {
                await sender.SendMessageAsync(DeadLetterResubmitter.CreateResubmitMessage(message), args.CancellationToken);
                await args.CompleteMessageAsync(message, args.CancellationToken);
                Interlocked.Increment(ref resubmitted);
                ConsoleUi.Success($"DLQ ▶ resubmitted #{message.SequenceNumber} {message.MessageId} (reason: {message.DeadLetterReason}).");
            }
            else
            {
                var path = await ArchiveAsync(entity, message, args.CancellationToken);
                await args.CompleteMessageAsync(message, args.CancellationToken);
                Interlocked.Increment(ref archived);
                ConsoleUi.Error($"DLQ ▶ ALERT #{message.SequenceNumber} {message.MessageId}: {decision} ({message.DeadLetterReason}). Archived to {path}.");
            }
        };
        processor.ProcessErrorAsync += args =>
        {
            ConsoleUi.Error($"[{args.ErrorSource}] {args.EntityPath}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync(CancellationToken.None);
        ConsoleUi.Info($"Listening on the dead-letter queue of '{entity}'. {StopSignal.Hint}");
        await StopSignal.WaitAsync(stopToken);
        await processor.StopProcessingAsync(CancellationToken.None);

        ConsoleUi.Table(["Resubmitted", "Archived"], [[resubmitted.ToString(), archived.ToString()]]);
    }

    private async Task<DlqSummary> ProcessDeadLettersAsync(
        ServiceBusEntity entity,
        IReadOnlyCollection<string>? retryableReasons,
        string? reasonFilter,
        bool archiveRejected,
        CancellationToken cancellationToken)
    {
        await using var receiver = CreateDlqReceiver(entity, ServiceBusReceiveMode.PeekLock);
        await using var sender = client.CreateSender(entity.SendPath);

        var summary = new DlqSummary();
        var seen = new HashSet<long>();
        var leftInDlq = new List<ServiceBusReceivedMessage>();

        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await receiver.ReceiveMessagesAsync(Options.BatchSize, TimeSpan.FromSeconds(3), cancellationToken);
            var fresh = messages.Where(m => seen.Add(m.SequenceNumber)).ToList();
            if (fresh.Count == 0)
            {
                // Either empty, or only messages we already decided to leave (their locks expired).
                break;
            }

            foreach (var message in fresh)
            {
                if (reasonFilter is not null && !string.Equals(message.DeadLetterReason, reasonFilter, StringComparison.OrdinalIgnoreCase))
                {
                    leftInDlq.Add(message);
                    summary.Skipped++;
                    continue;
                }

                var decision = DeadLetterResubmitter.Evaluate(message.DeadLetterReason, message.ApplicationProperties, Options.MaxResubmitAttempts, retryableReasons);
                switch (decision)
                {
                    case ResubmitDecision.Resubmit:
                        await sender.SendMessageAsync(DeadLetterResubmitter.CreateResubmitMessage(message), cancellationToken);
                        await receiver.CompleteMessageAsync(message, cancellationToken);
                        summary.Resubmitted++;
                        ConsoleUi.Success($"Resubmitted #{message.SequenceNumber} {message.MessageId} ({message.DeadLetterReason}).");
                        break;

                    case var _ when archiveRejected:
                        var path = await ArchiveAsync(entity, message, cancellationToken);
                        await receiver.CompleteMessageAsync(message, cancellationToken);
                        summary.Archived++;
                        ConsoleUi.Warning($"Archived #{message.SequenceNumber} {message.MessageId} ({decision}: {message.DeadLetterReason}) -> {path}");
                        break;

                    default:
                        leftInDlq.Add(message);
                        summary.Skipped++;
                        ConsoleUi.Warning($"Left in DLQ #{message.SequenceNumber} {message.MessageId}: {decision} ({message.DeadLetterReason}).");
                        break;
                }
            }
        }

        // Release the messages we chose not to touch so they are immediately visible again.
        foreach (var message in leftInDlq)
        {
            try
            {
                await receiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                // Lock already expired; the message is visible again anyway.
            }
        }

        ConsoleUi.Table(["Resubmitted", "Archived", "Left in DLQ"], [[summary.Resubmitted.ToString(), summary.Archived.ToString(), summary.Skipped.ToString()]]);
        return summary;
    }

    private ServiceBusReceiver CreateDlqReceiver(ServiceBusEntity entity, ServiceBusReceiveMode mode) =>
        entity.CreateReceiver(client, new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
            ReceiveMode = mode,
            PrefetchCount = Options.BatchSize,
        });

    private string ArchiveDirectory(ServiceBusEntity entity) =>
        Path.Combine(Path.GetFullPath(Options.ArchiveFolder, AppContext.BaseDirectory), entity.DisplayName.Replace('/', '_'));

    private async Task<string> ArchiveAsync(ServiceBusEntity entity, ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        var directory = ArchiveDirectory(entity);
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, $"{DateTime.UtcNow:yyyyMMddHHmmss}-{message.SequenceNumber}.json");

        var record = new
        {
            message.SequenceNumber,
            message.MessageId,
            message.CorrelationId,
            message.SessionId,
            message.Subject,
            message.ContentType,
            message.EnqueuedTime,
            message.DeliveryCount,
            message.DeadLetterReason,
            message.DeadLetterErrorDescription,
            message.DeadLetterSource,
            ApplicationProperties = message.ApplicationProperties.ToDictionary(p => p.Key, p => p.Value?.ToString()),
            Body = message.Body.ToString(),
            ArchivedAt = DateTimeOffset.UtcNow,
        };

        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(record, ArchiveJson), cancellationToken);
        return path;
    }
}

public sealed class DlqSummary
{
    public int Resubmitted { get; set; }

    public int Archived { get; set; }

    public int Skipped { get; set; }
}
