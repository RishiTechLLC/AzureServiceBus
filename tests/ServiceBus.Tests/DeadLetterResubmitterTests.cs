using Azure.Messaging.ServiceBus;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Tests;

public class DeadLetterResubmitterTests
{
    private static readonly string[] Retryable = ["MaxDeliveryCountExceeded", "TTLExpiredException"];

    private static ServiceBusReceivedMessage DeadLettered(string reason, Dictionary<string, object>? extra = null, string messageId = "ORD-1")
    {
        var properties = new Dictionary<string, object>
        {
            [MessageProperties.DeadLetterReason] = reason,
            [MessageProperties.DeadLetterErrorDescription] = "boom",
            [MessageProperties.SimulateFailure] = "transient",
            ["Custom"] = "keep-me",
        };
        foreach (var (k, v) in extra ?? [])
        {
            properties[k] = v;
        }

        return ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{\"orderId\":\"ORD-1\"}"),
            messageId: messageId,
            correlationId: "corr-1",
            subject: "OrderCreated",
            properties: properties,
            sequenceNumber: 42);
    }

    [Theory]
    [InlineData("MaxDeliveryCountExceeded", ResubmitDecision.Resubmit)]
    [InlineData("ttlexpiredexception", ResubmitDecision.Resubmit)]
    [InlineData("ValidationFailed", ResubmitDecision.NonRetryableReason)]
    [InlineData(null, ResubmitDecision.NonRetryableReason)]
    public void Evaluate_UsesRetryableReasons(string? reason, ResubmitDecision expected) =>
        Assert.Equal(expected, DeadLetterResubmitter.Evaluate(reason, new Dictionary<string, object>(), 3, Retryable));

    [Fact]
    public void Evaluate_WithoutReasonList_AcceptsAnyReason() =>
        Assert.Equal(ResubmitDecision.Resubmit, DeadLetterResubmitter.Evaluate("ValidationFailed", new Dictionary<string, object>(), 3));

    [Fact]
    public void Evaluate_StopsAtMaxResubmitAttempts()
    {
        var properties = new Dictionary<string, object> { [MessageProperties.ResubmitCount] = 3 };
        Assert.Equal(ResubmitDecision.MaxAttemptsReached, DeadLetterResubmitter.Evaluate("MaxDeliveryCountExceeded", properties, 3, Retryable));
    }

    [Fact]
    public void CreateResubmitMessage_PreservesPayloadAndMovesDeadLetterMetadata()
    {
        var message = DeadLetterResubmitter.CreateResubmitMessage(DeadLettered("MaxDeliveryCountExceeded"));

        Assert.Equal("{\"orderId\":\"ORD-1\"}", message.Body.ToString());
        Assert.Equal("corr-1", message.CorrelationId);
        Assert.Equal("OrderCreated", message.Subject);
        Assert.Equal("keep-me", message.ApplicationProperties["Custom"]);

        Assert.False(message.ApplicationProperties.ContainsKey(MessageProperties.DeadLetterReason));
        Assert.False(message.ApplicationProperties.ContainsKey(MessageProperties.DeadLetterErrorDescription));
        Assert.False(message.ApplicationProperties.ContainsKey(MessageProperties.SimulateFailure));

        Assert.Equal(1, message.ApplicationProperties[MessageProperties.ResubmitCount]);
        Assert.Equal("MaxDeliveryCountExceeded", message.ApplicationProperties[MessageProperties.OriginalDeadLetterReason]);
        Assert.Equal("boom", message.ApplicationProperties[MessageProperties.OriginalDeadLetterDescription]);
    }

    [Fact]
    public void CreateResubmitMessage_UsesNewMessageIdSoDuplicateDetectionDoesNotDropIt()
    {
        var first = DeadLetterResubmitter.CreateResubmitMessage(DeadLettered("X"));
        Assert.Equal("ORD-1-r1", first.MessageId);
        Assert.Equal("ORD-1", first.ApplicationProperties[MessageProperties.OriginalMessageId]);

        var second = DeadLetterResubmitter.CreateResubmitMessage(DeadLettered("X", new()
        {
            [MessageProperties.ResubmitCount] = 1,
            [MessageProperties.OriginalMessageId] = "ORD-1",
        }, messageId: "ORD-1-r1"));
        Assert.Equal("ORD-1-r2", second.MessageId);
        Assert.Equal(2, second.ApplicationProperties[MessageProperties.ResubmitCount]);
        Assert.Equal("ORD-1", second.ApplicationProperties[MessageProperties.OriginalMessageId]);
    }

    [Theory]
    [InlineData("abc", 1, "abc-r1")]
    [InlineData("abc-r1", 2, "abc-r2")]
    [InlineData("order-red", 1, "order-red-r1")]
    public void CreateResubmitMessageId_ReplacesExistingSuffix(string id, int count, string expected) =>
        Assert.Equal(expected, DeadLetterResubmitter.CreateResubmitMessageId(id, count));
}
