using System.Text.Json;
using System.Text.Json.Serialization;

namespace ServiceBus.Shared.Messaging;

/// <summary>Sample business payload carried in the message body as JSON.</summary>
public sealed record OrderMessage(
    string OrderId,
    string CustomerId,
    string Product,
    int Quantity,
    decimal Amount,
    DateTimeOffset CreatedAt)
{
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static readonly string[] Products = ["Laptop", "Phone", "Monitor", "Keyboard", "Headphones", "Tablet"];

    public static OrderMessage CreateSample(int index = 0)
    {
        var quantity = Random.Shared.Next(1, 5);
        return new OrderMessage(
            OrderId: $"ORD-{DateTime.UtcNow:yyyyMMddHHmmss}-{index:D4}-{Random.Shared.Next(1000, 9999)}",
            CustomerId: $"CUST-{Random.Shared.Next(1, 50):D3}",
            Product: Products[Random.Shared.Next(Products.Length)],
            Quantity: quantity,
            Amount: Math.Round(quantity * (decimal)(Random.Shared.NextDouble() * 900 + 100), 2),
            CreatedAt: DateTimeOffset.UtcNow);
    }
}
