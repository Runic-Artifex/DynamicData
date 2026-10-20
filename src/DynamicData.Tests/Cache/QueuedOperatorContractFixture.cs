namespace DynamicData.Tests.Cache;

/// <summary>
/// Two instances of an operator whose observers write synchronously into each other's input on two
/// threads. An operator that holds its gate while calling the observer deadlocks (ABBA) here.
/// </summary>
public class QueuedOperatorContractFixture
{
    private const int NonExpiring = 100;

    private static readonly TimeSpan Lifetime = TimeSpan.FromMilliseconds(10);

    [Test]
    [Arguments("to-observable-change-set")]
    public async Task ConcurrentCrossWritesReleaseOperatorGates(string operation)
    {
        var first = new DirectSource<int>();
        var second = new DirectSource<int>();
        using var rendezvous = new Barrier(2);
        var firstEntered = 0;
        var secondEntered = 0;
        var firstUpdates = 0;
        var secondUpdates = 0;
        Exception? firstError = null;
        Exception? secondError = null;
        var firstSubscription = Pipeline(first, operation).Subscribe(_ =>
        {
            Interlocked.Increment(ref firstUpdates);
            if (Interlocked.Exchange(ref firstEntered, 1) == 0)
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("The other producer did not enter its observer.");
                }

                second.Push(3);
            }
        }, error => firstError = error);
        var secondSubscription = Pipeline(second, operation).Subscribe(_ =>
        {
            Interlocked.Increment(ref secondUpdates);
            if (Interlocked.Exchange(ref secondEntered, 1) == 0)
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("The other producer did not enter its observer.");
                }

                first.Push(4);
            }
        }, error => secondError = error);

        // Dedicated threads establish real simultaneous observer ownership. Both observers
        // then write to the opposing input, reproducing the ABBA window of monitor gates.
        var firstWriter = Task.Factory.StartNew(() => first.Push(1), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var secondWriter = Task.Factory.StartNew(() => second.Push(2), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // A deadlocked delivery is left undisposed: disposal can wait for it and hang the run.
        await Task.WhenAll(firstWriter, secondWriter).WaitAsync(TimeSpan.FromSeconds(15));
        firstSubscription.Dispose();
        secondSubscription.Dispose();
        await Assert.That(firstError).IsNull();
        await Assert.That(secondError).IsNull();
        await Assert.That(firstUpdates).IsEqualTo(2);
        await Assert.That(secondUpdates).IsEqualTo(2);
    }

    [Test]
    [Arguments("to-observable-change-set")]
    public async Task ConcurrentCrossExpirationsReleaseOperatorGates(string operation)
    {
        var first = ExpiringPipeline(operation);
        var second = ExpiringPipeline(operation);
        using var rendezvous = new Barrier(2);
        var armed = 0;
        var firstEntered = 0;
        var secondEntered = 0;
        var firstUpdates = 0;
        var secondUpdates = 0;
        Exception? firstError = null;
        Exception? secondError = null;

        // Each observer, on its first expiration, waits until the other observer is running and then
        // writes a non-expiring item into the other operator's source.
        var firstSubscription = first.Emissions.Subscribe(_ =>
        {
            if (Volatile.Read(ref armed) == 0)
            {
                return;
            }

            Interlocked.Increment(ref firstUpdates);
            if (Interlocked.Exchange(ref firstEntered, 1) == 0)
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("The other expiration did not enter its observer.");
                }

                second.Write(NonExpiring + 2);
            }
        }, error => firstError = error);
        var secondSubscription = second.Emissions.Subscribe(_ =>
        {
            if (Volatile.Read(ref armed) == 0)
            {
                return;
            }

            Interlocked.Increment(ref secondUpdates);
            if (Interlocked.Exchange(ref secondEntered, 1) == 0)
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("The other expiration did not enter its observer.");
                }

                first.Write(NonExpiring + 1);
            }
        }, error => secondError = error);
        first.Write(1);
        second.Write(1);
        Volatile.Write(ref armed, 1);

        // Each scheduler is advanced on its own thread, so both expirations are delivered at once.
        var firstWriter = Task.Factory.StartNew(() => first.Scheduler.AdvanceBy(Lifetime.Ticks), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var secondWriter = Task.Factory.StartNew(() => second.Scheduler.AdvanceBy(Lifetime.Ticks), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // A deadlocked delivery is left undisposed: disposal can wait for it and hang the run.
        await Task.WhenAll(firstWriter, secondWriter).WaitAsync(TimeSpan.FromSeconds(15));
        firstSubscription.Dispose();
        secondSubscription.Dispose();
        first.Dispose();
        second.Dispose();
        await Assert.That(firstError).IsNull();
        await Assert.That(secondError).IsNull();

        // The expiration, then (for changeset operators) the other observer's write.
        await Assert.That(firstUpdates).IsEqualTo(first.UpdatesPerRound);
        await Assert.That(secondUpdates).IsEqualTo(second.UpdatesPerRound);
    }

    private static IObservable<int> Pipeline(IObservable<int> input, string operation) => operation switch
    {
        // ToObservableChangeSet publishes an initial empty changeset on subscription.
        "to-observable-change-set" => input.ToObservableChangeSet(static value => value).Skip(1).Select(static changes => changes.Count),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static TimeSpan? ExpireBelowNonExpiring(int value) => value < NonExpiring ? Lifetime : null;

    private static ExpiringOperator ExpiringPipeline(string operation)
    {
        var scheduler = new TestScheduler();
        var polling = operation.EndsWith("-polling", StringComparison.Ordinal) ? Lifetime : (TimeSpan?)null;
        switch (operation)
        {
            case "to-observable-change-set":
                {
                    var source = new DirectSource<int>();
                    return new(
                        source.ToObservableChangeSet(static value => value, ExpireBelowNonExpiring, scheduler: scheduler).Select(static changes => changes.Count),
                        source.Push,
                        scheduler,
                        UpdatesPerRound: 2,
                        Disposable.Empty);
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    private sealed record ExpiringOperator(IObservable<int> Emissions, Action<int> Write, TestScheduler Scheduler, int UpdatesPerRound, IDisposable Owner) : IDisposable
    {
        public void Dispose() => Owner.Dispose();
    }

    private sealed class DirectSource<T> : IObservable<T>
    {
        private IObserver<T>? _observer;

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return Disposable.Create(() => _observer = null);
        }

        public void Push(T value) => _observer?.OnNext(value);
    }
}
