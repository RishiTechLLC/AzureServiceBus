using Azure;
using Azure.Messaging.ServiceBus.Administration;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Messaging;
using ServiceBus.Shared.Console;

namespace ServiceBus.Shared.Administration;

/// <summary>Creates the sample entities and reports their runtime message counts.</summary>
public sealed class EntityManager(ServiceBusAdministrationClient admin, ServiceBusSettings settings)
{
    /// <summary>Creates the queue, session queue, topic and subscription if they do not exist yet.</summary>
    public async Task EnsureEntitiesAsync(CancellationToken cancellationToken = default)
    {
        await EnsureQueueAsync(settings.QueueName, requiresSession: false, cancellationToken);
        await EnsureQueueAsync(settings.SessionQueueName, requiresSession: true, cancellationToken);

        if (!await admin.TopicExistsAsync(settings.TopicName, cancellationToken))
        {
            await admin.CreateTopicAsync(new CreateTopicOptions(settings.TopicName)
            {
                RequiresDuplicateDetection = true,
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10),
            }, cancellationToken);
            ConsoleUi.Success($"Created topic '{settings.TopicName}'.");
        }
        else
        {
            ConsoleUi.Info($"Topic '{settings.TopicName}' already exists.");
        }

        if (!await admin.SubscriptionExistsAsync(settings.TopicName, settings.SubscriptionName, cancellationToken))
        {
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(settings.TopicName, settings.SubscriptionName)
            {
                MaxDeliveryCount = 5,
                LockDuration = TimeSpan.FromSeconds(30),
                DeadLetteringOnMessageExpiration = true,
                EnableDeadLetteringOnFilterEvaluationExceptions = true,
            }, cancellationToken);
            ConsoleUi.Success($"Created subscription '{settings.TopicName}/{settings.SubscriptionName}'.");
        }
        else
        {
            ConsoleUi.Info($"Subscription '{settings.TopicName}/{settings.SubscriptionName}' already exists.");
        }
    }

    private async Task EnsureQueueAsync(string name, bool requiresSession, CancellationToken cancellationToken)
    {
        if (await admin.QueueExistsAsync(name, cancellationToken))
        {
            ConsoleUi.Info($"Queue '{name}' already exists.");
            return;
        }

        await admin.CreateQueueAsync(new CreateQueueOptions(name)
        {
            RequiresSession = requiresSession,
            MaxDeliveryCount = 5,
            LockDuration = TimeSpan.FromSeconds(30),
            DefaultMessageTimeToLive = TimeSpan.FromDays(7),
            DeadLetteringOnMessageExpiration = true,
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10),
        }, cancellationToken);

        ConsoleUi.Success($"Created queue '{name}' (sessions: {requiresSession}, max delivery count: 5, dead-letter on expiry).");
    }

    /// <summary>Prints active / dead-letter / scheduled counts for all configured entities.</summary>
    public async Task PrintStatisticsAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<string[]>();

        foreach (var queue in new[] { settings.QueueName, settings.SessionQueueName })
        {
            rows.Add(await GetQueueRowAsync(queue, cancellationToken));
        }

        rows.Add(await GetSubscriptionRowAsync(ServiceBusEntity.Subscription(settings.TopicName, settings.SubscriptionName), cancellationToken));

        ConsoleUi.Table(["Entity", "Active", "Dead-letter", "Scheduled", "Transfer DLQ", "Total"], rows);
    }

    private async Task<string[]> GetQueueRowAsync(string queue, CancellationToken cancellationToken)
    {
        try
        {
            QueueRuntimeProperties p = await admin.GetQueueRuntimePropertiesAsync(queue, cancellationToken);
            return [queue, p.ActiveMessageCount.ToString(), p.DeadLetterMessageCount.ToString(), p.ScheduledMessageCount.ToString(), p.TransferDeadLetterMessageCount.ToString(), p.TotalMessageCount.ToString()];
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return [queue, "not found", "-", "-", "-", "-"];
        }
    }

    private async Task<string[]> GetSubscriptionRowAsync(ServiceBusEntity entity, CancellationToken cancellationToken)
    {
        try
        {
            SubscriptionRuntimeProperties p = await admin.GetSubscriptionRuntimePropertiesAsync(entity.QueueOrTopicName, entity.SubscriptionName, cancellationToken);
            return [entity.DisplayName, p.ActiveMessageCount.ToString(), p.DeadLetterMessageCount.ToString(), "-", p.TransferDeadLetterMessageCount.ToString(), p.TotalMessageCount.ToString()];
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return [entity.DisplayName, "not found", "-", "-", "-", "-"];
        }
    }
}
