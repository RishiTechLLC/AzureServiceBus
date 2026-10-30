using System.Globalization;
using Azure.Messaging.ServiceBus;

namespace ServiceBus.Shared.Console;

using Console = System.Console;

/// <summary>Small helpers for colored console output and prompts.</summary>
public static class ConsoleUi
{
    private static readonly Lock Sync = new();

    /// <summary>True once standard input has been closed (e.g. piped input ran out).</summary>
    public static bool EndOfInput { get; private set; }

    public static void Header(string text)
    {
        Write(ConsoleColor.Cyan, Environment.NewLine + new string('=', text.Length + 4));
        Write(ConsoleColor.Cyan, $"  {text}");
        Write(ConsoleColor.Cyan, new string('=', text.Length + 4));
    }

    public static void Info(string text) => Write(ConsoleColor.Gray, text);

    public static void Success(string text) => Write(ConsoleColor.Green, "✔ " + text);

    public static void Warning(string text) => Write(ConsoleColor.Yellow, "⚠ " + text);

    public static void Error(string text) => Write(ConsoleColor.Red, "✖ " + text);

    public static void Write(ConsoleColor color, string text)
    {
        lock (Sync)
        {
            var previous = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = previous;
        }
    }

    public static string Prompt(string label, string? defaultValue = null)
    {
        Console.Write(defaultValue is null ? $"{label}: " : $"{label} [{defaultValue}]: ");
        var input = Console.ReadLine();
        EndOfInput |= input is null;
        return string.IsNullOrWhiteSpace(input) ? defaultValue ?? string.Empty : input.Trim();
    }

    public static int PromptInt(string label, int defaultValue, int min = 1, int max = int.MaxValue)
    {
        while (true)
        {
            var text = Prompt(label, defaultValue.ToString(CultureInfo.InvariantCulture));
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value >= min && value <= max)
            {
                return value;
            }

            Warning($"Enter a whole number between {min} and {max}.");
        }
    }

    public static long PromptLong(string label)
    {
        while (true)
        {
            if (long.TryParse(Prompt(label), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                return value;
            }

            Warning("Enter a valid number.");
        }
    }

    public static bool Confirm(string question)
    {
        var answer = Prompt($"{question} (y/N)", "N");
        return answer.Equals("y", StringComparison.OrdinalIgnoreCase) || answer.Equals("yes", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Shows a numbered menu and returns the key of the chosen item.</summary>
    public static string Menu(string title, IReadOnlyList<(string Key, string Label)> items)
    {
        Header(title);
        foreach (var (key, label) in items)
        {
            Console.WriteLine($"  {key,3}) {label}");
        }

        while (true)
        {
            var choice = Prompt(Environment.NewLine + "Select an option");
            if (EndOfInput)
            {
                return "q";
            }

            if (items.Any(i => i.Key.Equals(choice, StringComparison.OrdinalIgnoreCase)))
            {
                return choice.ToLowerInvariant();
            }

            Warning("Unknown option, try again.");
        }
    }

    public static void Table(IReadOnlyList<string> headers, IReadOnlyList<string[]> rows)
    {
        var widths = headers.Select((h, i) => Math.Max(h.Length, rows.Count == 0 ? 0 : rows.Max(r => r[i].Length))).ToArray();
        string Line(IReadOnlyList<string> cells) => string.Join(" | ", cells.Select((c, i) => c.PadRight(widths[i])));

        lock (Sync)
        {
            Console.WriteLine();
            Console.WriteLine(Line(headers));
            Console.WriteLine(string.Join("-+-", widths.Select(w => new string('-', w))));
            foreach (var row in rows)
            {
                Console.WriteLine(Line(row));
            }

            Console.WriteLine();
        }
    }

    /// <summary>Prints the useful metadata and body of a received message.</summary>
    public static void PrintMessage(ServiceBusReceivedMessage message, bool includeBody = true)
    {
        lock (Sync)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"── Message #{message.SequenceNumber} ─────────────────────────────");
            Console.ResetColor();
            Console.WriteLine($"  MessageId     : {message.MessageId}");
            Console.WriteLine($"  Subject       : {message.Subject}");
            Console.WriteLine($"  CorrelationId : {message.CorrelationId}");
            if (!string.IsNullOrEmpty(message.SessionId))
            {
                Console.WriteLine($"  SessionId     : {message.SessionId}");
            }

            Console.WriteLine($"  Enqueued      : {message.EnqueuedTime:u}");
            Console.WriteLine($"  DeliveryCount : {message.DeliveryCount}");
            Console.WriteLine($"  State         : {message.State}");

            if (!string.IsNullOrEmpty(message.DeadLetterReason))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"  DLQ Reason    : {message.DeadLetterReason}");
                Console.WriteLine($"  DLQ Details   : {message.DeadLetterErrorDescription}");
                Console.WriteLine($"  DLQ Source    : {message.DeadLetterSource ?? "(this entity)"}");
                Console.ResetColor();
            }

            foreach (var (key, value) in message.ApplicationProperties)
            {
                Console.WriteLine($"  [{key}] = {value}");
            }

            if (includeBody)
            {
                Console.WriteLine($"  Body          : {message.Body}");
            }
        }
    }
}
