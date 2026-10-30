using System.Globalization;
using Azure.Messaging.ServiceBus;
using ServiceBus.Receiver;
using ServiceBus.Receiver.DeadLetter;
using ServiceBus.Receiver.Processing;
using ServiceBus.Shared.Administration;
using ServiceBus.Shared.Configuration;
using ServiceBus.Shared.Console;
using ServiceBus.Shared.Messaging;

ServiceBusSettings settings;
try
{
    settings = AppConfiguration.Load(args, typeof(OrderProcessor).Assembly);
}
catch (InvalidOperationException ex)
{
    ConsoleUi.Error(ex.Message);
    return 1;
}

// Ctrl+C stops the current long-running operation; a second Ctrl+C at the menu exits.
var operationCts = new CancellationTokenSource();
var exitRequested = false;
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    if (operationCts.IsCancellationRequested)
    {
        exitRequested = true;
    }

    operationCts.Cancel();
};

await using var client = ServiceBusFactory.CreateClient(settings);
var deferredStore = new DeferredMessageStore();
var processor = new OrderProcessor(client, settings, deferredStore);
var manual = new ManualReceiver(client, deferredStore);
var deadLetters = new DeadLetterService(client, settings);
var entities = new EntityManager(ServiceBusFactory.CreateAdministrationClient(settings), settings);

var queue = ServiceBusEntity.Queue(settings.QueueName);
var subscription = ServiceBusEntity.Subscription(settings.TopicName, settings.SubscriptionName);
var useSubscription = args.Any(a => a.Equals("--target=subscription", StringComparison.OrdinalIgnoreCase));
ServiceBusEntity Current() => useSubscription ? subscription : queue;

ConsoleUi.Header("Azure Service Bus - Receiver");
ConsoleUi.Info($"Namespace: {client.FullyQualifiedNamespace}  |  Auth: {(settings.UsesConnectionString ? "Connection string" : "Microsoft Entra ID")}");

// Non-interactive mode: dotnet run -- <command> [arg] [--target=subscription]
var positional = args.Where(a => !a.StartsWith("--", StringComparison.Ordinal)).ToArray();
if (positional.Length > 0)
{
    return await RunAsync(positional[0].ToLowerInvariant(), positional.Skip(1).ToArray(), interactive: false) ? 0 : 1;
}

while (!exitRequested)
{
    var choice = ConsoleUi.Menu($"Receiver menu  (entity: {Current()})",
    [
        ("1", "Process messages continuously (processor)"),
        ("2", "Process session queue (ordered, per-session)"),
        ("3", "Receive & settle manually (complete / release / defer / dead-letter)"),
        ("4", "Peek messages (non-destructive)"),
        ("5", "Receive deferred messages"),
        ("6", "DLQ: peek dead-lettered messages"),
        ("7", "DLQ: resubmit messages to main entity"),
        ("8", "DLQ: auto-process using retry policy"),
        ("9", "DLQ: listen continuously (auto-resubmit / archive + alert)"),
        ("10", "DLQ: archive to disk and purge"),
        ("11", "DLQ: purge (permanently delete)"),
        ("e", $"Switch entity to {(useSubscription ? queue : subscription)}"),
        ("s", "Show entity statistics"),
        ("q", "Quit"),
    ]);

    if (choice == "q")
    {
        break;
    }

    if (choice == "e")
    {
        useSubscription = !useSubscription;
        continue;
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

    bool Confirmed(string question) => !interactive || ConsoleUi.Confirm(question);

    if (operationCts.IsCancellationRequested)
    {
        operationCts.Dispose();
        operationCts = new CancellationTokenSource();
    }

    var token = operationCts.Token;
    var entity = Current();

    try
    {
        switch (command)
        {
            case "1" or "process":
                await processor.RunAsync(entity, token);
                break;
            case "2" or "sessions":
                await processor.RunSessionsAsync(token);
                break;
            case "3" or "receive":
                await manual.ReceiveInteractivelyAsync(entity, IntArg(0, "Max messages", 5), token);
                break;
            case "4" or "peek":
                await manual.PeekAsync(entity, IntArg(0, "Max messages", 10), SubQueue.None, token);
                break;
            case "5" or "deferred":
                await manual.ReceiveDeferredAsync(entity, token);
                break;
            case "6" or "dlq-peek":
                await manual.PeekAsync(entity, IntArg(0, "Max messages", 20), SubQueue.DeadLetter, token);
                break;
            case "7" or "dlq-resubmit":
                var reason = commandArgs.Length > 0 ? commandArgs[0] : interactive ? ConsoleUi.Prompt("Only this dead-letter reason (blank = all)") : string.Empty;
                if (Confirmed($"Resubmit dead-lettered messages{(reason.Length > 0 ? $" with reason '{reason}'" : "")} to '{entity.SendPath}'?"))
                {
                    await deadLetters.ResubmitAsync(entity, reason.Length > 0 ? reason : null, token);
                }

                break;
            case "8" or "dlq-auto":
                ConsoleUi.Info($"Retryable reasons: {string.Join(", ", settings.DeadLetter.RetryableReasons)}; max resubmits: {settings.DeadLetter.MaxResubmitAttempts}.");
                var archive = commandArgs.Contains("archive") || (interactive && ConsoleUi.Confirm("Archive and remove messages that cannot be resubmitted?"));
                await deadLetters.AutoProcessAsync(entity, archive, token);
                break;
            case "9" or "dlq-listen":
                await deadLetters.ListenAsync(entity, token);
                break;
            case "10" or "dlq-archive":
                if (Confirmed("Archive every DLQ message to disk and remove it from the DLQ?"))
                {
                    await deadLetters.ArchiveAndPurgeAsync(entity, token);
                }

                break;
            case "11" or "dlq-purge":
                if (Confirmed($"Permanently delete ALL messages in the dead-letter queue of '{entity}'?"))
                {
                    await deadLetters.PurgeAsync(entity, token);
                }

                break;
            case "s" or "stats":
                await entities.PrintStatisticsAsync(token);
                break;
            default:
                ConsoleUi.Error($"Unknown command '{command}'. Commands: process, sessions, receive [n], peek [n], deferred, dlq-peek [n], dlq-resubmit [reason], dlq-auto [archive], dlq-listen, dlq-archive, dlq-purge, stats. Add --target=subscription to use the topic subscription.");
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
    catch (Exception ex) when (ex is Azure.RequestFailedException or Azure.Identity.AuthenticationFailedException or UnauthorizedAccessException or IOException)
    {
        ConsoleUi.Error(ex.Message);
    }

    return false;
}
