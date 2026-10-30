namespace ServiceBus.Shared.Configuration;

/// <summary>
/// Strongly typed settings bound from the "ServiceBus" configuration section.
/// Authenticate with either <see cref="ConnectionString"/> (SAS) or
/// <see cref="FullyQualifiedNamespace"/> (Microsoft Entra ID via DefaultAzureCredential).
/// </summary>
public sealed class ServiceBusSettings
{
    public const string SectionName = "ServiceBus";

    /// <summary>SAS connection string. Takes precedence over <see cref="FullyQualifiedNamespace"/>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>e.g. "my-namespace.servicebus.windows.net". Used with DefaultAzureCredential.</summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>Use AMQP over WebSockets (port 443) when 5671 is blocked by a firewall.</summary>
    public bool UseWebSockets { get; set; }

    public string QueueName { get; set; } = "orders";

    /// <summary>Session-enabled queue used for FIFO / grouped processing.</summary>
    public string SessionQueueName { get; set; } = "orders-sessions";

    public string TopicName { get; set; } = "order-events";

    public string SubscriptionName { get; set; } = "all-orders";

    public ProcessingSettings Processing { get; set; } = new();

    public DeadLetterSettings DeadLetter { get; set; } = new();

    public RetrySettings Retry { get; set; } = new();

    public bool UsesConnectionString => !string.IsNullOrWhiteSpace(ConnectionString);

    /// <summary>Returns the list of configuration problems; empty when valid.</summary>
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(ConnectionString) && string.IsNullOrWhiteSpace(FullyQualifiedNamespace))
        {
            errors.Add($"Set either {SectionName}:ConnectionString or {SectionName}:FullyQualifiedNamespace.");
        }

        if (string.IsNullOrWhiteSpace(QueueName))
        {
            errors.Add($"{SectionName}:QueueName is required.");
        }

        if (Processing.MaxConcurrentCalls < 1)
        {
            errors.Add($"{SectionName}:Processing:MaxConcurrentCalls must be at least 1.");
        }

        if (Processing.PrefetchCount < 0)
        {
            errors.Add($"{SectionName}:Processing:PrefetchCount cannot be negative.");
        }

        if (DeadLetter.MaxResubmitAttempts < 1)
        {
            errors.Add($"{SectionName}:DeadLetter:MaxResubmitAttempts must be at least 1.");
        }

        return errors;
    }
}

public sealed class ProcessingSettings
{
    public int MaxConcurrentCalls { get; set; } = 4;

    public int PrefetchCount { get; set; } = 10;

    public int MaxConcurrentSessions { get; set; } = 4;

    /// <summary>How long the processor keeps renewing the message lock while a handler is running.</summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long to wait for a new message in a session before moving to the next session.</summary>
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Simulated work time per message so lock renewal / concurrency can be observed.</summary>
    public TimeSpan SimulatedWorkDuration { get; set; } = TimeSpan.FromMilliseconds(250);
}

public sealed class DeadLetterSettings
{
    /// <summary>A dead-lettered message is not resubmitted more than this many times (poison protection).</summary>
    public int MaxResubmitAttempts { get; set; } = 3;

    /// <summary>Number of messages fetched per DLQ receive call.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>Dead-letter reasons considered transient and therefore safe to resubmit automatically.</summary>
    public List<string> RetryableReasons { get; set; } = ["MaxDeliveryCountExceeded", "TTLExpiredException", "TransientFailure"];

    /// <summary>Folder where messages that cannot be resubmitted are archived as JSON before removal.</summary>
    public string ArchiveFolder { get; set; } = "dlq-archive";
}

public sealed class RetrySettings
{
    public int MaxRetries { get; set; } = 5;

    public TimeSpan Delay { get; set; } = TimeSpan.FromSeconds(0.8);

    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan TryTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
