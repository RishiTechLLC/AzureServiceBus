using ServiceBus.Receiver.Processing;
using ServiceBus.Shared.Messaging;

namespace ServiceBus.Tests;

public class OrderHandlerTests
{
    private static BinaryData ValidBody(int quantity = 2, decimal amount = 10m) =>
        BinaryData.FromObjectAsJson(OrderMessage.CreateSample() with { Quantity = quantity, Amount = amount }, OrderMessage.JsonOptions);

    private static Task<HandlingResult> Handle(BinaryData body, Dictionary<string, object>? properties = null) =>
        OrderHandler.HandleAsync(body, properties ?? [], TimeSpan.Zero, CancellationToken.None);

    [Fact]
    public async Task ValidOrder_IsCompleted()
    {
        var result = await Handle(ValidBody());
        Assert.Equal(HandlingOutcome.Complete, result.Outcome);
        Assert.NotNull(result.Order);
    }

    [Theory]
    [InlineData("{ not json")]
    [InlineData("{\"customerId\":\"C1\"}")]
    public async Task MalformedBody_IsDeadLettered(string body)
    {
        var result = await Handle(BinaryData.FromString(body));
        Assert.Equal(HandlingOutcome.DeadLetter, result.Outcome);
        Assert.Equal("DeserializationFailed", result.Reason);
    }

    [Fact]
    public async Task InvalidQuantity_IsDeadLettered()
    {
        var result = await Handle(ValidBody(quantity: 0));
        Assert.Equal(HandlingOutcome.DeadLetter, result.Outcome);
        Assert.Equal("ValidationFailed", result.Reason);
    }

    [Theory]
    [InlineData("fatal", HandlingOutcome.DeadLetter)]
    [InlineData("transient", HandlingOutcome.Release)]
    public async Task SimulatedFailures(string kind, HandlingOutcome expected)
    {
        var result = await Handle(ValidBody(), new() { [MessageProperties.SimulateFailure] = kind });
        Assert.Equal(expected, result.Outcome);
    }

    [Fact]
    public async Task DeferFlag_DefersMessage()
    {
        var result = await Handle(ValidBody(), new() { [MessageProperties.SimulateDefer] = true });
        Assert.Equal(HandlingOutcome.Defer, result.Outcome);
    }

    [Fact]
    public void MessageBuilder_RoundTripsOrder()
    {
        var order = OrderMessage.CreateSample(7);
        var message = MessageBuilder.FromOrder(order, sessionId: "S1");

        Assert.Equal(order.OrderId, message.MessageId);
        Assert.Equal("S1", message.SessionId);
        Assert.Equal(MessageProperties.JsonContentType, message.ContentType);
        Assert.True(MessageBuilder.TryReadOrder(message.Body, out var read, out _));
        Assert.Equal(order, read);
    }
}
