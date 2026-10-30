using Azure.Messaging.ServiceBus;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Receiver.Processing;

/// <summary>Continuous, concurrent message processing with ServiceBusProcessor / ServiceBusSessionProcessor.</summary>
public sealed class OrderProcessor(ServiceBusClient client, ServiceBusSettings settings, DeferredMessageStore deferredStore)
{
    private int _completed, _deadLettered, _deferred, _released;

    /// <summary>Processes messages from a queue or subscription until <paramref name="stopToken"/> fires.</summary>
    public async Task RunAsync(ServiceBusEntity entity, CancellationToken stopToken)
    {
        await using var processor = entity.CreateProcessor(client, new ServiceBusProcessorOptions
        {
            // Settle explicitly so each outcome (complete / release / defer / dead-letter) is visible.
            AutoCompleteMessages = false,
            MaxConcurrentCalls = settings.Processing.MaxConcurrentCalls,
            PrefetchCount = settings.Processing.PrefetchCount,
            MaxAutoLockRenewalDuration = settings.Processing.MaxAutoLockRenewalDuration,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        processor.ProcessMessageAsync += args => HandleAsync(entity.DisplayName, args.Message, new ProcessorSettlement(args), args.CancellationToken);
        processor.ProcessErrorAsync += OnErrorAsync;

        await RunUntilStoppedAsync(entity.DisplayName, processor.StartProcessingAsync, processor.StopProcessingAsync, stopToken);
    }

    /// <summary>
    /// Processes a session-enabled queue. Messages within a session are delivered in order to a
    /// single handler at a time; session state is used to track progress per session.
    /// </summary>
    public async Task RunSessionsAsync(CancellationToken stopToken)
    {
        await using var processor = client.CreateSessionProcessor(settings.SessionQueueName, new ServiceBusSessionProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentSessions = settings.Processing.MaxConcurrentSessions,
            MaxConcurrentCallsPerSession = 1,
            SessionIdleTimeout = settings.Processing.SessionIdleTimeout,
            MaxAutoLockRenewalDuration = settings.Processing.MaxAutoLockRenewalDuration,
            PrefetchCount = settings.Processing.PrefetchCount,
        });

        processor.SessionInitializingAsync += args =>
        {
            ConsoleUi.Write(ConsoleColor.DarkCyan, $"▶ Session '{args.SessionId}' acquired.");
            return Task.CompletedTask;
        };
        processor.SessionClosingAsync += args =>
        {
            ConsoleUi.Write(ConsoleColor.DarkCyan, $"■ Session '{args.SessionId}' released.");
            return Task.CompletedTask;
        };
        processor.ProcessMessageAsync += async args =>
        {
            await HandleAsync(settings.SessionQueueName, args.Message, new SessionSettlement(args), args.CancellationToken);

            // Persist progress in session state so another receiver can resume where this one stopped.
            var state = (await args.GetSessionStateAsync(args.CancellationToken))?.ToObjectFromJson<SessionProgress>() ?? new SessionProgress(0, null);
            await args.SetSessionStateAsync(BinaryData.FromObjectAsJson(new SessionProgress(state.Processed + 1, args.Message.MessageId)), args.CancellationToken);
        };
        processor.ProcessErrorAsync += OnErrorAsync;

        await RunUntilStoppedAsync(settings.SessionQueueName + " (sessions)", processor.StartProcessingAsync, processor.StopProcessingAsync, stopToken);
    }

    private async Task HandleAsync(string entityName, ServiceBusReceivedMessage message, ISettlement settle, CancellationToken cancellationToken)
    {
        var result = await OrderHandler.HandleAsync(message.Body, message.ApplicationProperties, settings.Processing.SimulatedWorkDuration, cancellationToken);
        var label = $"#{message.SequenceNumber} {message.MessageId}" + (string.IsNullOrEmpty(message.SessionId) ? "" : $" [session {message.SessionId}]");

        switch (result.Outcome)
        {
            case HandlingOutcome.Complete:
                await settle.CompleteAsync(message, cancellationToken);
                Interlocked.Increment(ref _completed);
                ConsoleUi.Success($"Completed {label}: {result.Order!.Product} x{result.Order.Quantity} for {result.Order.CustomerId}.");
                break;

            case HandlingOutcome.Defer:
                await settle.DeferAsync(message, cancellationToken);
                deferredStore.Add(entityName, message.SequenceNumber);
                Interlocked.Increment(ref _deferred);
                ConsoleUi.Write(ConsoleColor.Magenta, $"⏸ Deferred {label}. Retrieve it later with 'Receive deferred messages'.");
                break;

            case HandlingOutcome.DeadLetter:
                await settle.DeadLetterAsync(message, result.Reason!, result.Description!, cancellationToken);
                Interlocked.Increment(ref _deadLettered);
                ConsoleUi.Error($"Dead-lettered {label}: {result.Reason} - {result.Description}");
                break;

            case HandlingOutcome.Release:
                // Abandon releases the lock so the message is redelivered. Once DeliveryCount exceeds the
                // entity's MaxDeliveryCount the broker moves it to the DLQ (reason MaxDeliveryCountExceeded).
                await settle.AbandonAsync(message, new Dictionary<string, object> { [MessageProperties.LastError] = result.Description! }, cancellationToken);
                Interlocked.Increment(ref _released);
                ConsoleUi.Warning($"Released {label} (delivery {message.DeliveryCount}): {result.Description}");
                break;
        }
    }

    private static Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        var reason = args.Exception is ServiceBusException sbe ? $"{sbe.Reason}, transient: {sbe.IsTransient}" : args.Exception.GetType().Name;
        ConsoleUi.Error($"[{args.ErrorSource}] {args.EntityPath}: {reason} - {args.Exception.Message}");
        return Task.CompletedTask;
    }

    private async Task RunUntilStoppedAsync(string name, Func<CancellationToken, Task> start, Func<CancellationToken, Task> stop, CancellationToken stopToken)
    {
        _completed = _deadLettered = _deferred = _released = 0;
        await start(CancellationToken.None);
        ConsoleUi.Info($"Processing '{name}'. {StopSignal.Hint}");

        await StopSignal.WaitAsync(stopToken);

        ConsoleUi.Info("Stopping processor (in-flight messages are allowed to finish)...");
        await stop(CancellationToken.None);
        ConsoleUi.Table(["Completed", "Released", "Deferred", "Dead-lettered"],
            [[_completed.ToString(), _released.ToString(), _deferred.ToString(), _deadLettered.ToString()]]);
    }

    private sealed record SessionProgress(int Processed, string? LastMessageId);

    // Small adapters so the same handling code works for both processor types.
    private interface ISettlement
    {
        Task CompleteAsync(ServiceBusReceivedMessage m, CancellationToken ct);
        Task AbandonAsync(ServiceBusReceivedMessage m, IDictionary<string, object> props, CancellationToken ct);
        Task DeferAsync(ServiceBusReceivedMessage m, CancellationToken ct);
        Task DeadLetterAsync(ServiceBusReceivedMessage m, string reason, string description, CancellationToken ct);
    }

    private sealed class ProcessorSettlement(ProcessMessageEventArgs a) : ISettlement
    {
        public Task CompleteAsync(ServiceBusReceivedMessage m, CancellationToken ct) => a.CompleteMessageAsync(m, ct);
        public Task AbandonAsync(ServiceBusReceivedMessage m, IDictionary<string, object> props, CancellationToken ct) => a.AbandonMessageAsync(m, props, ct);
        public Task DeferAsync(ServiceBusReceivedMessage m, CancellationToken ct) => a.DeferMessageAsync(m, cancellationToken: ct);
        public Task DeadLetterAsync(ServiceBusReceivedMessage m, string reason, string description, CancellationToken ct) => a.DeadLetterMessageAsync(m, reason, description, ct);
    }

    private sealed class SessionSettlement(ProcessSessionMessageEventArgs a) : ISettlement
    {
        public Task CompleteAsync(ServiceBusReceivedMessage m, CancellationToken ct) => a.CompleteMessageAsync(m, ct);
        public Task AbandonAsync(ServiceBusReceivedMessage m, IDictionary<string, object> props, CancellationToken ct) => a.AbandonMessageAsync(m, props, ct);
        public Task DeferAsync(ServiceBusReceivedMessage m, CancellationToken ct) => a.DeferMessageAsync(m, cancellationToken: ct);
        public Task DeadLetterAsync(ServiceBusReceivedMessage m, string reason, string description, CancellationToken ct) => a.DeadLetterMessageAsync(m, reason, description, ct);
    }
}
