using System.Globalization;
using Azure.Messaging.ServiceBus;
using ServiceBus.Sender;
using ServiceBus.Shared.Administration;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

ServiceBusSettings settings;
try
{
    settings = AppConfiguration.Load(args, typeof(OrderSender).Assembly);
}
catch (InvalidOperationException ex)
{
    ConsoleUi.Error(ex.Message);
    return 1;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await using var client = ServiceBusFactory.CreateClient(settings);
await using var sender = new OrderSender(client, settings);
var entities = new EntityManager(ServiceBusFactory.CreateAdministrationClient(settings), settings);

ConsoleUi.Header("Azure Service Bus - Sender");
ConsoleUi.Info($"Namespace: {client.FullyQualifiedNamespace}  |  Auth: {(settings.UsesConnectionString ? "Connection string" : "Microsoft Entra ID")}");

// Non-interactive mode: dotnet run -- <command> [arg] (e.g. "batch 100").
var positional = args.Where(a => !a.StartsWith("--", StringComparison.Ordinal)).ToArray();
if (positional.Length > 0)
{
    return await RunAsync(positional[0].ToLowerInvariant(), positional.Skip(1).ToArray(), interactive: false) ? 0 : 1;
}

(string Key, string Label)[] menu =
[
    ("1", "Send a single message"),
    ("2", "Send a batch of messages"),
    ("3", "Schedule a message"),
    ("4", "Cancel a scheduled message"),
    ("5", "Send session (ordered) messages"),
    ("6", "Publish to topic"),
    ("7", "Send transient-failure message (-> DLQ after max delivery count)"),
    ("8", "Send fatal-failure message (-> DLQ immediately)"),
    ("9", "Send malformed message (-> DLQ, deserialization failure)"),
    ("10", "Send message to be deferred"),
    ("11", "Send message with short TTL (-> DLQ on expiry)"),
    ("12", "Send duplicate message (duplicate detection demo)"),
    ("s", "Show entity statistics"),
    ("c", "Create entities (queues, topic, subscription)"),
    ("q", "Quit"),
];

while (!cts.IsCancellationRequested)
{
    var choice = ConsoleUi.Menu("Sender menu", menu);
    if (choice == "q")
    {
        break;
    }

    await RunAsync(choice, [], interactive: true);
}

return 0;

async Task<bool> RunAsync(string command, string[] commandArgs, bool interactive)
{
    int IntArg(int index, string label, int defaultValue) =>
        commandArgs.Length > index && int.TryParse(commandArgs[index], NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)
            ? v
            : interactive ? ConsoleUi.PromptInt(label, defaultValue) : defaultValue;

    var token = cts.Token;
    try
    {
        switch (command)
        {
            case "1" or "send":
                await sender.SendSingleAsync(token);
                break;
            case "2" or "batch":
                await sender.SendBatchAsync(IntArg(0, "How many messages", 10), token);
                break;
            case "3" or "schedule":
                await sender.ScheduleAsync(TimeSpan.FromSeconds(IntArg(0, "Delay in seconds", 60)), token);
                break;
            case "4" or "cancel":
                var sequenceNumber = commandArgs.Length > 0 ? long.Parse(commandArgs[0], CultureInfo.InvariantCulture) : ConsoleUi.PromptLong("Sequence number");
                await sender.CancelScheduledAsync(sequenceNumber, token);
                break;
            case "5" or "session":
                await sender.SendSessionMessagesAsync(IntArg(0, "Number of sessions", 3), IntArg(1, "Messages per session", 5), token);
                break;
            case "6" or "topic":
                await sender.PublishToTopicAsync(IntArg(0, "How many messages", 5), token);
                break;
            case "7" or "transient":
                await sender.SendFailingMessageAsync("transient", token);
                break;
            case "8" or "fatal":
                await sender.SendFailingMessageAsync("fatal", token);
                break;
            case "9" or "malformed":
                await sender.SendMalformedMessageAsync(token);
                break;
            case "10" or "defer":
                await sender.SendDeferrableMessageAsync(token);
                break;
            case "11" or "ttl":
                await sender.SendExpiringMessageAsync(TimeSpan.FromSeconds(IntArg(0, "Time-to-live in seconds", 10)), token);
                break;
            case "12" or "duplicate":
                await sender.SendDuplicateAsync(token);
                break;
            case "s" or "stats":
                await entities.PrintStatisticsAsync(token);
                break;
            case "c" or "setup":
                await entities.EnsureEntitiesAsync(token);
                break;
            default:
                ConsoleUi.Error($"Unknown command '{command}'. Commands: send, batch [n], schedule [sec], cancel <seq>, session [sessions] [perSession], topic [n], transient, fatal, malformed, defer, ttl [sec], duplicate, stats, setup.");
                return false;
        }

        return true;
    }
    catch (ServiceBusException ex)
    {
        ConsoleUi.Error($"Service Bus error ({ex.Reason}, transient: {ex.IsTransient}): {ex.Message}");
    }
    catch (OperationCanceledException)
    {
        ConsoleUi.Warning("Operation cancelled.");
    }
    catch (Exception ex) when (ex is Azure.RequestFailedException or Azure.Identity.AuthenticationFailedException or UnauthorizedAccessException or FormatException)
    {
        ConsoleUi.Error(ex.Message);
    }

    return false;
}
