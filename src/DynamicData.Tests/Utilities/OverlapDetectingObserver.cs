namespace DynamicData.Tests.Utilities;

/// <summary>
/// Records whether any two notifications overlapped and whether anything followed the terminal one.
/// </summary>
/// <typeparam name="T">The type of the values.</typeparam>
public sealed class OverlapDetectingObserver<T>(Action<T>? onNext = null) : IObserver<T>
{
    private readonly TaskCompletionSource _terminated = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _active;
    private int _maxActive;
    private int _afterTerminal;
    private int _terminalCount;

    public int MaxConcurrentCalls => Volatile.Read(ref _maxActive);

    public int CallsAfterTerminal => Volatile.Read(ref _afterTerminal);

    public int TerminalCount => Volatile.Read(ref _terminalCount);

    public int Values { get; private set; }

    public Exception? Error { get; private set; }

    public bool HasCompleted { get; private set; }

    public Task Terminated => _terminated.Task;

    public void OnNext(T value) => Run(() =>
    {
        Values++;
        onNext?.Invoke(value);
    }, terminal: false);

    public void OnError(Exception error) => Run(() => Error = error, terminal: true);

    public void OnCompleted() => Run(() => HasCompleted = true, terminal: true);

    private void Run(Action action, bool terminal)
    {
        var active = Interlocked.Increment(ref _active);
        InterlockedMax(ref _maxActive, active);

        if (Volatile.Read(ref _terminalCount) != 0)
        {
            Interlocked.Increment(ref _afterTerminal);
        }

        try
        {
            action();
        }
        finally
        {
            if (terminal)
            {
                Interlocked.Increment(ref _terminalCount);
            }

            Interlocked.Decrement(ref _active);

            if (terminal)
            {
                _terminated.TrySetResult();
            }
        }
    }

    private static void InterlockedMax(ref int location, int value)
    {
        int current;
        while ((current = Volatile.Read(ref location)) < value && Interlocked.CompareExchange(ref location, value, current) != current)
        {
        }
    }
}
