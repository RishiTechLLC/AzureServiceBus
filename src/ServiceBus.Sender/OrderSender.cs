using Azure.Messaging.ServiceBus;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Sender;

/// <summary>All the ways this sample publishes messages to Service Bus.</summary>
public sealed class OrderSender(ServiceBusClient client, ServiceBusSettings settings) : IAsyncDisposable
{
    private readonly ServiceBusSender _queueSender = client.CreateSender(settings.QueueName);
    private readonly ServiceBusSender _sessionSender = client.CreateSender(settings.SessionQueueName);
    private readonly ServiceBusSender _topicSender = client.CreateSender(settings.TopicName);

    /// <summary>Sends a single order message to the queue.</summary>
    public async Task SendSingleAsync(CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        var message = MessageBuilder.FromOrder(order);
        await _queueSender.SendMessageAsync(message, cancellationToken);
        ConsoleUi.Success($"Sent {order.OrderId} ({order.Product} x{order.Quantity}, {order.Amount:C}) to '{settings.QueueName}'.");
    }

    /// <summary>Sends many messages efficiently, splitting into as many size-limited batches as needed.</summary>
    public Task SendBatchAsync(int count, CancellationToken cancellationToken) =>
        SendInBatchesAsync(_queueSender, Enumerable.Range(1, count).Select(i => MessageBuilder.FromOrder(OrderMessage.CreateSample(i))), cancellationToken);

    /// <summary>Publishes messages to the topic; every subscription receives a copy.</summary>
    public Task PublishToTopicAsync(int count, CancellationToken cancellationToken) =>
        SendInBatchesAsync(_topicSender, Enumerable.Range(1, count).Select(i => MessageBuilder.FromOrder(OrderMessage.CreateSample(i))), cancellationToken);

    /// <summary>Schedules a message for future delivery and returns its sequence number (needed to cancel).</summary>
    public async Task<long> ScheduleAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        var enqueueAt = DateTimeOffset.UtcNow.Add(delay);
        var sequenceNumber = await _queueSender.ScheduleMessageAsync(MessageBuilder.FromOrder(order), enqueueAt, cancellationToken);
        ConsoleUi.Success($"Scheduled {order.OrderId} for {enqueueAt:u}. Sequence number: {sequenceNumber} (use it to cancel).");
        return sequenceNumber;
    }

    public async Task CancelScheduledAsync(long sequenceNumber, CancellationToken cancellationToken)
    {
        await _queueSender.CancelScheduledMessageAsync(sequenceNumber, cancellationToken);
        ConsoleUi.Success($"Cancelled scheduled message {sequenceNumber}.");
    }

    /// <summary>
    /// Sends ordered messages grouped by session (e.g. one session per customer). The receiver
    /// processes each session's messages in FIFO order with an exclusive lock.
    /// </summary>
    public async Task SendSessionMessagesAsync(int sessions, int messagesPerSession, CancellationToken cancellationToken)
    {
        var messages = new List<ServiceBusMessage>();
        for (var s = 1; s <= sessions; s++)
        {
            var sessionId = $"CUST-{s:D3}";
            for (var m = 1; m <= messagesPerSession; m++)
            {
                var message = MessageBuilder.FromOrder(OrderMessage.CreateSample(m) with { CustomerId = sessionId }, sessionId);
                message.ApplicationProperties["SequenceInSession"] = m;
                messages.Add(message);
            }
        }

        // A batch may only contain messages for one session, so batch per session.
        foreach (var group in messages.GroupBy(m => m.SessionId))
        {
            await SendInBatchesAsync(_sessionSender, group, cancellationToken, quiet: true);
        }

        ConsoleUi.Success($"Sent {messages.Count} messages across {sessions} session(s) to '{settings.SessionQueueName}'.");
    }

    /// <summary>
    /// Sends a message the receiver will fail on. "transient" failures are retried until
    /// MaxDeliveryCount is exceeded and the broker dead-letters them; "fatal" ones are
    /// dead-lettered immediately by the receiver.
    /// </summary>
    public async Task SendFailingMessageAsync(string failureKind, CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        var message = MessageBuilder.FromOrder(order);
        message.ApplicationProperties[MessageProperties.SimulateFailure] = failureKind;
        await _queueSender.SendMessageAsync(message, cancellationToken);
        ConsoleUi.Success($"Sent {order.OrderId} flagged with {MessageProperties.SimulateFailure}={failureKind}.");
    }

    /// <summary>Sends a message whose body is not a valid order, so the receiver dead-letters it.</summary>
    public async Task SendMalformedMessageAsync(CancellationToken cancellationToken)
    {
        var message = new ServiceBusMessage("{ this is : not valid json")
        {
            MessageId = Guid.NewGuid().ToString("N"),
            ContentType = MessageProperties.JsonContentType,
            Subject = MessageProperties.OrderCreatedSubject,
        };
        await _queueSender.SendMessageAsync(message, cancellationToken);
        ConsoleUi.Success($"Sent malformed message {message.MessageId}.");
    }

    /// <summary>Sends a message the receiver will defer (set aside to be fetched later by sequence number).</summary>
    public async Task SendDeferrableMessageAsync(CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        var message = MessageBuilder.FromOrder(order);
        message.ApplicationProperties[MessageProperties.SimulateDefer] = true;
        await _queueSender.SendMessageAsync(message, cancellationToken);
        ConsoleUi.Success($"Sent {order.OrderId} flagged for deferral.");
    }

    /// <summary>
    /// Sends a message with a short time-to-live. If nobody receives it in time it expires and,
    /// because the queue has dead-lettering on expiration, lands in the DLQ with reason TTLExpiredException.
    /// </summary>
    public async Task SendExpiringMessageAsync(TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        var message = MessageBuilder.FromOrder(order);
        message.TimeToLive = timeToLive;
        await _queueSender.SendMessageAsync(message, cancellationToken);
        ConsoleUi.Success($"Sent {order.OrderId} with TTL {timeToLive}.");
    }

    /// <summary>Sends the same MessageId twice; with duplicate detection enabled only one is enqueued.</summary>
    public async Task SendDuplicateAsync(CancellationToken cancellationToken)
    {
        var order = OrderMessage.CreateSample();
        await _queueSender.SendMessageAsync(MessageBuilder.FromOrder(order), cancellationToken);
        await _queueSender.SendMessageAsync(MessageBuilder.FromOrder(order), cancellationToken);
        ConsoleUi.Success($"Sent {order.OrderId} twice with the same MessageId. Duplicate detection keeps only one.");
    }

    private static async Task SendInBatchesAsync(ServiceBusSender sender, IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken, bool quiet = false)
    {
        var pending = new Queue<ServiceBusMessage>(messages);
        var total = pending.Count;
        var batches = 0;

        while (pending.Count > 0)
        {
            using var batch = await sender.CreateMessageBatchAsync(cancellationToken);

            while (pending.Count > 0 && batch.TryAddMessage(pending.Peek()))
            {
                pending.Dequeue();
            }

            if (batch.Count == 0)
            {
                // A single message larger than the maximum batch size: send it on its own (fails with a clear error if too large).
                await sender.SendMessageAsync(pending.Dequeue(), cancellationToken);
            }
            else
            {
                await sender.SendMessagesAsync(batch, cancellationToken);
            }

            batches++;
        }

        if (!quiet)
        {
            ConsoleUi.Success($"Sent {total} message(s) to '{sender.EntityPath}' in {batches} batch(es).");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _queueSender.DisposeAsync();
        await _sessionSender.DisposeAsync();
        await _topicSender.DisposeAsync();
    }
}
