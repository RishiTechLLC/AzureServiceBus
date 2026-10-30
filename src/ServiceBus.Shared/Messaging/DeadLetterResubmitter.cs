using System.Globalization;
using Azure.Messaging.ServiceBus;

namespace ServiceBus.Shared.Messaging;

public enum ResubmitDecision
{
    /// <summary>Message can be sent back to the main entity.</summary>
    Resubmit,

    /// <summary>Message has already been resubmitted the maximum number of times.</summary>
    MaxAttemptsReached,

    /// <summary>Dead-letter reason is not considered transient.</summary>
    NonRetryableReason,
}

/// <summary>Pure logic for deciding whether and how a dead-lettered message is resubmitted.</summary>
public static class DeadLetterResubmitter
{
    public static int GetResubmitCount(IReadOnlyDictionary<string, object> properties) =>
        properties.TryGetValue(MessageProperties.ResubmitCount, out var value) && value is not null
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : 0;

    /// <summary>
    /// Decides whether a dead-lettered message should be resubmitted.
    /// When <paramref name="retryableReasons"/> is null every reason is accepted (manual resubmit).
    /// </summary>
    public static ResubmitDecision Evaluate(
        string? deadLetterReason,
        IReadOnlyDictionary<string, object> properties,
        int maxResubmitAttempts,
        IReadOnlyCollection<string>? retryableReasons = null)
    {
        if (GetResubmitCount(properties) >= maxResubmitAttempts)
        {
            return ResubmitDecision.MaxAttemptsReached;
        }

        if (retryableReasons is not null &&
            !retryableReasons.Contains(deadLetterReason ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            return ResubmitDecision.NonRetryableReason;
        }

        return ResubmitDecision.Resubmit;
    }

    /// <summary>
    /// Creates a copy of a dead-lettered message suitable for sending back to the main entity:
    /// body, ids and properties are preserved, dead-letter metadata is moved to "Original*"
    /// properties and the resubmit counter is incremented.
    /// </summary>
    public static ServiceBusMessage CreateResubmitMessage(ServiceBusReceivedMessage deadLettered, TimeProvider? timeProvider = null)
    {
        var message = new ServiceBusMessage(deadLettered);
        ApplyResubmitMetadata(message.ApplicationProperties, deadLettered.DeadLetterReason, deadLettered.DeadLetterErrorDescription, timeProvider);

        // Duplicate detection would silently drop a resubmit that reuses the original MessageId
        // inside the detection window, so give each resubmission its own id.
        message.MessageId = CreateResubmitMessageId(deadLettered.MessageId, GetResubmitCount(message.ApplicationProperties.AsReadOnly()));
        message.ApplicationProperties.TryAdd(MessageProperties.OriginalMessageId, deadLettered.MessageId);
        return message;
    }

    /// <summary>"ORD-1" -> "ORD-1-r1"; "ORD-1-r1" -> "ORD-1-r2".</summary>
    internal static string CreateResubmitMessageId(string originalMessageId, int resubmitCount)
    {
        var marker = originalMessageId.LastIndexOf("-r", StringComparison.Ordinal);
        var baseId = marker > 0 && int.TryParse(originalMessageId.AsSpan(marker + 2), NumberStyles.None, CultureInfo.InvariantCulture, out _)
            ? originalMessageId[..marker]
            : originalMessageId;
        return $"{baseId}-r{resubmitCount}";
    }

    internal static void ApplyResubmitMetadata(
        IDictionary<string, object> properties,
        string? deadLetterReason,
        string? deadLetterDescription,
        TimeProvider? timeProvider = null)
    {
        var count = properties.TryGetValue(MessageProperties.ResubmitCount, out var value) && value is not null
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : 0;

        properties.Remove(MessageProperties.DeadLetterReason);
        properties.Remove(MessageProperties.DeadLetterErrorDescription);

        // Clear the simulation flag so a resubmitted "transient" failure can now succeed.
        properties.Remove(MessageProperties.SimulateFailure);

        properties[MessageProperties.ResubmitCount] = count + 1;
        properties[MessageProperties.OriginalDeadLetterReason] = deadLetterReason ?? string.Empty;
        properties[MessageProperties.OriginalDeadLetterDescription] = deadLetterDescription ?? string.Empty;
        properties[MessageProperties.ResubmittedAt] = (timeProvider ?? TimeProvider.System).GetUtcNow().ToString("O", CultureInfo.InvariantCulture);
    }
}
