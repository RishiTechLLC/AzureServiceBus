using ServiceBus.Shared.Messaging;

namespace ServiceBus.Receiver.Processing;

public enum HandlingOutcome
{
    Complete,
    Defer,
    DeadLetter,
    Release,
}

public sealed record HandlingResult(HandlingOutcome Outcome, string? Reason = null, string? Description = null, OrderMessage? Order = null);

/// <summary>
/// Business logic shared by every receive mode. It only decides what should happen to a
/// message; the caller performs the settlement (complete / abandon / defer / dead-letter).
/// </summary>
public static class OrderHandler
{
    public static async Task<HandlingResult> HandleAsync(
        BinaryData body,
        IReadOnlyDictionary<string, object> properties,
        TimeSpan simulatedWork,
        CancellationToken cancellationToken)
    {
        // Poison message: retrying a body that cannot be parsed will never succeed.
        if (!MessageBuilder.TryReadOrder(body, out var order, out var error))
        {
            return new HandlingResult(HandlingOutcome.DeadLetter, "DeserializationFailed", error);
        }

        if (properties.TryGetValue(MessageProperties.SimulateDefer, out var defer) && defer is true)
        {
            return new HandlingResult(HandlingOutcome.Defer, Order: order);
        }

        if (properties.TryGetValue(MessageProperties.SimulateFailure, out var failure))
        {
            switch (failure?.ToString())
            {
                case "fatal":
                    return new HandlingResult(HandlingOutcome.DeadLetter, "ValidationFailed", $"Order {order!.OrderId} failed business validation.", order);
                case "transient":
                    return new HandlingResult(HandlingOutcome.Release, "TransientFailure", "Downstream dependency unavailable (simulated).", order);
            }
        }

        if (order!.Quantity <= 0 || order.Amount <= 0)
        {
            return new HandlingResult(HandlingOutcome.DeadLetter, "ValidationFailed", "Quantity and amount must be positive.", order);
        }

        await Task.Delay(simulatedWork, cancellationToken);
        return new HandlingResult(HandlingOutcome.Complete, Order: order);
    }
}
