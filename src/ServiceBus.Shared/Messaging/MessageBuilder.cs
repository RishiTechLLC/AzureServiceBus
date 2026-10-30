using System.Text.Json;
using Azure.Messaging.ServiceBus;

namespace ServiceBus.Shared.Messaging;

/// <summary>Builds <see cref="ServiceBusMessage"/> instances with consistent metadata.</summary>
public static class MessageBuilder
{
    public static ServiceBusMessage FromOrder(OrderMessage order, string? sessionId = null, string source = "ServiceBus.Sender")
    {
        var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(order, OrderMessage.JsonOptions))
        {
            // A deterministic MessageId lets duplicate detection drop accidental re-sends.
            MessageId = order.OrderId,
            ContentType = MessageProperties.JsonContentType,
            Subject = MessageProperties.OrderCreatedSubject,
            CorrelationId = Guid.NewGuid().ToString("N"),
            SessionId = sessionId,
        };

        message.ApplicationProperties[MessageProperties.MessageType] = MessageProperties.OrderCreatedSubject;
        message.ApplicationProperties[MessageProperties.Source] = source;
        return message;
    }

    /// <summary>Attempts to read an <see cref="OrderMessage"/> from a received message body.</summary>
    public static bool TryReadOrder(BinaryData body, out OrderMessage? order, out string? error)
    {
        try
        {
            order = body.ToObjectFromJson<OrderMessage>(OrderMessage.JsonOptions);
            if (order is null || string.IsNullOrWhiteSpace(order.OrderId))
            {
                error = "Body does not contain a valid order (missing orderId).";
                order = null;
                return false;
            }

            error = null;
            return true;
        }
        catch (JsonException ex)
        {
            order = null;
            error = $"Body is not valid JSON: {ex.Message}";
            return false;
        }
    }
}
