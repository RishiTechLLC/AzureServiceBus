namespace ServiceBus.Receiver;

/// <summary>Waits for the user to stop a long-running receive loop.</summary>
internal static class StopSignal
{
    public static string Hint => Console.IsInputRedirected ? "Press Ctrl+C to stop." : "Press Enter (or Ctrl+C) to stop.";

    public static async Task WaitAsync(CancellationToken stopToken)
    {
        var cancelled = Task.Delay(Timeout.Infinite, stopToken).ContinueWith(_ => { }, TaskScheduler.Default);

        if (Console.IsInputRedirected)
        {
            await cancelled;
            return;
        }

        await Task.WhenAny(Task.Run(Console.ReadLine, CancellationToken.None), cancelled);
    }
}
