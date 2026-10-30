namespace ServiceBus.Shared.Messaging;

/// <summary>Application property names shared by the sender and receiver.</summary>
public static class MessageProperties
{
    /// <summary>Content type of <see cref="OrderMessage"/> bodies.</summary>
    public const string JsonContentType = "application/json";

    /// <summary>Subject (label) used for order messages.</summary>
    public const string OrderCreatedSubject = "OrderCreated";

    /// <summary>Message type discriminator, e.g. "OrderCreated".</summary>
    public const string MessageType = "MessageType";

    /// <summary>Name of the application that produced the message.</summary>
    public const string Source = "Source";

    /// <summary>Simulation flag: "transient" (fail until max delivery count) or "fatal" (dead-letter immediately).</summary>
    public const string SimulateFailure = "SimulateFailure";

    /// <summary>Simulation flag: when true the receiver defers the message.</summary>
    public const string SimulateDefer = "SimulateDefer";

    /// <summary>How many times the message has been resubmitted from the dead-letter queue.</summary>
    public const string ResubmitCount = "ResubmitCount";

    /// <summary>The dead-letter reason recorded before the last resubmission.</summary>
    public const string OriginalDeadLetterReason = "OriginalDeadLetterReason";

    /// <summary>The dead-letter description recorded before the last resubmission.</summary>
    public const string OriginalDeadLetterDescription = "OriginalDeadLetterDescription";

    /// <summary>MessageId of the message before its first resubmission.</summary>
    public const string OriginalMessageId = "OriginalMessageId";

    /// <summary>Last processing error, recorded when a message lock is released (abandoned).</summary>
    public const string LastError = "LastError";

    /// <summary>Time (UTC) of the last resubmission.</summary>
    public const string ResubmittedAt = "ResubmittedAt";

    // Broker-populated application properties on dead-lettered messages.
    public const string DeadLetterReason = "DeadLetterReason";
    public const string DeadLetterErrorDescription = "DeadLetterErrorDescription";
}
