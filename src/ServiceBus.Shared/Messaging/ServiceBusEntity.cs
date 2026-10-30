using Azure.Messaging.ServiceBus;

namespace ServiceBus.Shared.Messaging;

/// <summary>A queue, or a topic subscription, that messages can be received from.</summary>
public sealed record ServiceBusEntity(string QueueOrTopicName, string? SubscriptionName = null)
{
    public static ServiceBusEntity Queue(string name) => new(name);

    public static ServiceBusEntity Subscription(string topic, string subscription) => new(topic, subscription);

    public bool IsSubscription => SubscriptionName is not null;

    /// <summary>Path of the entity that new / resubmitted messages are sent to.</summary>
    public string SendPath => QueueOrTopicName;

    public string DisplayName => IsSubscription ? $"{QueueOrTopicName}/subscriptions/{SubscriptionName}" : QueueOrTopicName;

    public ServiceBusReceiver CreateReceiver(ServiceBusClient client, ServiceBusReceiverOptions? options = null) =>
        IsSubscription
            ? client.CreateReceiver(QueueOrTopicName, SubscriptionName, options ?? new())
            : client.CreateReceiver(QueueOrTopicName, options ?? new());

    public ServiceBusProcessor CreateProcessor(ServiceBusClient client, ServiceBusProcessorOptions options) =>
        IsSubscription
            ? client.CreateProcessor(QueueOrTopicName, SubscriptionName, options)
            : client.CreateProcessor(QueueOrTopicName, options);

    public override string ToString() => DisplayName;
}
