using Azure.Messaging.ServiceBus;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Receiver.Processing;

/// <summary>Pull-based receive operations where the user decides how each message is settled.</summary>
public sealed class ManualReceiver(ServiceBusClient client, DeferredMessageStore deferredStore)
{
    /// <summary>
    /// Receives messages in peek-lock mode and lets the user complete, release (abandon),
    /// defer or dead-letter each one.
    /// </summary>
    public async Task ReceiveInteractivelyAsync(ServiceBusEntity entity, int maxMessages, CancellationToken cancellationToken)
    {
        await using var receiver = entity.CreateReceiver(client, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        var messages = await receiver.ReceiveMessagesAsync(maxMessages, TimeSpan.FromSeconds(5), cancellationToken);

        if (messages.Count == 0)
        {
            ConsoleUi.Info($"No messages available on '{entity}'.");
            return;
        }

        ConsoleUi.Info($"Received {messages.Count} message(s). Locks are held until settled or until they expire ({messages[0].LockedUntil:T}).");

        foreach (var message in messages)
        {
            ConsoleUi.PrintMessage(message);
            var action = ConsoleUi.Prompt("[c]omplete, [r]elease (abandon), [d]efer, [x] dead-letter, [s]kip", "c").ToLowerInvariant();

            try
            {
                // Lock may have expired while the user was reading; renewing keeps it valid.
                if (action != "s" && message.LockedUntil < DateTimeOffset.UtcNow.AddSeconds(5))
                {
                    await receiver.RenewMessageLockAsync(message, cancellationToken);
                }

                switch (action)
                {
                    case "r":
                        await receiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);
                        ConsoleUi.Warning("Released: the message is immediately available to be received again (DeliveryCount + 1).");
                        break;
                    case "d":
                        await receiver.DeferMessageAsync(message, cancellationToken: cancellationToken);
                        deferredStore.Add(entity.DisplayName, message.SequenceNumber);
                        ConsoleUi.Write(ConsoleColor.Magenta, $"Deferred. Sequence number {message.SequenceNumber} saved for later retrieval.");
                        break;
                    case "x":
                        var reason = ConsoleUi.Prompt("Dead-letter reason", "ManualDeadLetter");
                        var description = ConsoleUi.Prompt("Description", "Dead-lettered by operator");
                        await receiver.DeadLetterMessageAsync(message, reason, description, cancellationToken);
                        ConsoleUi.Error($"Moved to the dead-letter queue ({reason}).");
                        break;
                    case "s":
                        ConsoleUi.Info("Skipped: the lock expires and the message becomes available again.");
                        break;
                    default:
                        await receiver.CompleteMessageAsync(message, cancellationToken);
                        ConsoleUi.Success("Completed and removed from the queue.");
                        break;
                }
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                ConsoleUi.Error("Lock lost - the message will be redelivered.");
            }
        }
    }

    /// <summary>Browses messages without locking or removing them.</summary>
    public async Task PeekAsync(ServiceBusEntity entity, int maxMessages, SubQueue subQueue, CancellationToken cancellationToken)
    {
        await using var receiver = entity.CreateReceiver(client, new ServiceBusReceiverOptions { SubQueue = subQueue });
        var messages = await receiver.PeekMessagesAsync(maxMessages, cancellationToken: cancellationToken);
        var name = subQueue == SubQueue.DeadLetter ? $"{entity} (dead-letter queue)" : entity.DisplayName;

        if (messages.Count == 0)
        {
            ConsoleUi.Info($"'{name}' is empty.");
            return;
        }

        ConsoleUi.Info($"Peeked {messages.Count} message(s) from '{name}':");
        foreach (var message in messages)
        {
            ConsoleUi.PrintMessage(message);
        }

        if (subQueue == SubQueue.DeadLetter)
        {
            ConsoleUi.Table(["Dead-letter reason", "Count"],
                messages.GroupBy(m => m.DeadLetterReason ?? "(none)").Select(g => new[] { g.Key, g.Count().ToString() }).ToList());
        }
    }

    /// <summary>Retrieves previously deferred messages by sequence number and completes them.</summary>
    public async Task ReceiveDeferredAsync(ServiceBusEntity entity, CancellationToken cancellationToken)
    {
        var sequenceNumbers = deferredStore.GetAll(entity.DisplayName);
        if (sequenceNumbers.Count == 0)
        {
            ConsoleUi.Info("No deferred sequence numbers recorded for this entity.");
            return;
        }

        await using var receiver = entity.CreateReceiver(client);
        var handled = new List<long>();

        foreach (var chunk in sequenceNumbers.Chunk(50))
        {
            IReadOnlyList<ServiceBusReceivedMessage> messages;
            try
            {
                messages = await receiver.ReceiveDeferredMessagesAsync(chunk, cancellationToken);
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageNotFound)
            {
                // At least one was already settled elsewhere or expired; fall back to one at a time.
                messages = [];
                foreach (var sequenceNumber in chunk)
                {
                    try
                    {
                        messages = [.. messages, .. await receiver.ReceiveDeferredMessagesAsync([sequenceNumber], cancellationToken)];
                    }
                    catch (ServiceBusException inner) when (inner.Reason == ServiceBusFailureReason.MessageNotFound)
                    {
                        ConsoleUi.Warning($"Deferred message {sequenceNumber} no longer exists.");
                        handled.Add(sequenceNumber);
                    }
                }
            }

            foreach (var message in messages)
            {
                await receiver.CompleteMessageAsync(message, cancellationToken);
                handled.Add(message.SequenceNumber);
                ConsoleUi.Success($"Processed deferred message #{message.SequenceNumber} ({message.MessageId}).");
            }
        }

        deferredStore.Remove(entity.DisplayName, handled);
    }
}
