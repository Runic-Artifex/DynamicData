namespace DynamicData.Tests;

/// <summary>
/// Disposal of operators that deliver through a queue rather than under their gate. Disposing must not
/// wait for a delivery that is in progress: the observer may be the caller, or may be waiting for a lock
/// the caller holds. It stops further delivery and lets the in-flight one finish.
/// </summary>
public class QueuedOperatorDisposalFixture
{
    private const int NonExpiring = 100;

    private static readonly TimeSpan Lifetime = TimeSpan.FromMilliseconds(10);

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    [Test]
    [Arguments("list-expire-after")]
    [Arguments("cache-expire-after-source")]
    [Arguments("cache-expire-after-stream")]
    [Arguments("list-to-observable-change-set")]
    [Arguments("cache-to-observable-change-set")]
    [Arguments("list-filter-predicate-state")]
    public async Task DisposeInsideDeliveryReturnsAndStopsDelivery(string operation)
    {
        var operatorCase = CreateCase(operation);
        var updates = 0;
        var lateUpdates = 0;
        var disposeReturned = 0;
        IDisposable? subscription = null;
        subscription = operatorCase.Emissions.Subscribe(_ =>
        {
            if (Volatile.Read(ref disposeReturned) != 0)
            {
                Interlocked.Increment(ref lateUpdates);
            }

            if (Interlocked.Increment(ref updates) == 1)
            {
                subscription!.Dispose();
                Volatile.Write(ref disposeReturned, 1);
            }
        });

        // The first delivery disposes from inside the observer, on the delivering thread.
        await RunOnDedicatedThread(() =>
        {
            operatorCase.Trigger(1);
            operatorCase.Trigger(2);
        });
        operatorCase.Dispose();

        await Assert.That(disposeReturned).IsEqualTo(1);
        await Assert.That(updates).IsEqualTo(1);
        await Assert.That(lateUpdates).IsEqualTo(0);
    }

    [Test]
    [Arguments("list-expire-after")]
    [Arguments("cache-expire-after-source")]
    [Arguments("cache-expire-after-stream")]
    [Arguments("list-to-observable-change-set")]
    [Arguments("cache-to-observable-change-set")]
    [Arguments("list-filter-predicate-state")]
    public async Task DisposeDuringDeliveryOnAnotherThreadReturnsAndStopsDelivery(string operation)
    {
        var operatorCase = CreateCase(operation);
        var entered = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var updates = 0;
        var lateUpdates = 0;
        var disposeReturned = 0;
        var subscription = operatorCase.Emissions.Subscribe(_ =>
        {
            if (Volatile.Read(ref disposeReturned) != 0)
            {
                Interlocked.Increment(ref lateUpdates);
            }

            if (Interlocked.Increment(ref updates) == 1)
            {
                entered.Set();
                release.Wait(Timeout);
            }
        });

        var producer = StartDedicatedThread(() => operatorCase.Trigger(1));
        await RunOnDedicatedThread(() =>
        {
            if (!entered.Wait(Timeout))
            {
                throw new TimeoutException("The delivery did not start.");
            }
        });

        // Dispose while the observer is still running on the producer thread, then let it finish.
        var disposer = StartDedicatedThread(() =>
        {
            subscription.Dispose();
            Volatile.Write(ref disposeReturned, 1);
        });
        await Task.WhenAny(disposer, Task.Delay(200));
        release.Set();

        // A hung disposal is left as is: waiting for it would hang the run.
        await Task.WhenAll(producer, disposer).WaitAsync(Timeout);
        await RunOnDedicatedThread(() => operatorCase.Trigger(2));
        operatorCase.Dispose();

        await Assert.That(updates).IsEqualTo(1);
        await Assert.That(lateUpdates).IsEqualTo(0);
    }

    [Test]
    [Arguments("list-expire-after")]
    [Arguments("cache-expire-after-source")]
    [Arguments("cache-expire-after-stream")]
    [Arguments("list-to-observable-change-set")]
    [Arguments("cache-to-observable-change-set")]
    [Arguments("list-filter-predicate-state")]
    public async Task DisposeUnderSourceLockWhileObserverWritesToSourceDoesNotDeadlock(string operation)
    {
        var operatorCase = CreateCase(operation);
        var entered = new ManualResetEventSlim();
        var lockHeld = new ManualResetEventSlim();
        var updates = 0;
        var lateUpdates = 0;
        var disposeReturned = 0;
        var subscription = operatorCase.Emissions.Subscribe(_ =>
        {
            if (Volatile.Read(ref disposeReturned) != 0)
            {
                Interlocked.Increment(ref lateUpdates);
            }

            if (Interlocked.Increment(ref updates) == 1)
            {
                entered.Set();
                if (!lockHeld.Wait(Timeout))
                {
                    throw new TimeoutException("The disposer did not take the source lock.");
                }

                // Blocks until the disposer releases the source lock.
                operatorCase.Write(NonExpiring + 1);
            }
        });

        var producer = StartDedicatedThread(() => operatorCase.Trigger(1));
        await RunOnDedicatedThread(() =>
        {
            if (!entered.Wait(Timeout))
            {
                throw new TimeoutException("The delivery did not start.");
            }
        });

        // Dispose while holding the lock the observer is about to need.
        var disposer = StartDedicatedThread(() => operatorCase.UnderSourceLock(() =>
        {
            lockHeld.Set();
            subscription.Dispose();
            Volatile.Write(ref disposeReturned, 1);
        }));

        // A deadlocked disposal is left as is: waiting for it would hang the run.
        await Task.WhenAll(producer, disposer).WaitAsync(Timeout);
        operatorCase.Dispose();

        await Assert.That(updates).IsEqualTo(1);
        await Assert.That(lateUpdates).IsEqualTo(0);
    }

    private static Task StartDedicatedThread(Action action)
        => Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private static Task RunOnDedicatedThread(Action action)
        => StartDedicatedThread(action).WaitAsync(Timeout);

    private static TimeSpan? ExpireBelowNonExpiring(int value) => value < NonExpiring ? Lifetime : null;

    private static OperatorCase CreateCase(string operation)
    {
        var scheduler = new TestScheduler();
        switch (operation)
        {
            case "list-expire-after":
                {
                    var source = new SourceList<int>();
                    return new(
                        source.ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Select(static items => items.Count()),
                        source.Add,
                        action => source.Edit(_ => action()),
                        scheduler,
                        source);
                }

            case "cache-expire-after-source":
                {
                    var source = new SourceCache<int, int>(static value => value);
                    return new(
                        source.ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Select(static items => items.Count()),
                        source.AddOrUpdate,
                        action => source.Edit(_ => action()),
                        scheduler,
                        source);
                }

            case "cache-expire-after-stream":
                {
                    var source = new SourceCache<int, int>(static value => value);
                    return new(
                        source.Connect().ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Select(static changes => changes.Count),
                        source.AddOrUpdate,
                        action => source.Edit(_ => action()),
                        scheduler,
                        source);
                }

            case "list-to-observable-change-set":
                {
                    var source = new LockedSource<int>();
                    return new(
                        source.ToObservableChangeSet().Select(static changes => changes.Count).Where(static count => count != 0),
                        source.Write,
                        source.UnderLock,
                        scheduler,
                        Disposable.Empty,
                        source.Push);
                }

            case "cache-to-observable-change-set":
                {
                    var source = new LockedSource<int>();
                    return new(
                        source.ToObservableChangeSet(static value => value).Select(static changes => changes.Count).Where(static count => count != 0),
                        source.Write,
                        source.UnderLock,
                        scheduler,
                        Disposable.Empty,
                        source.Push);
                }

            case "list-filter-predicate-state":
                {
                    var source = new SourceList<int>();
                    return new(
                        source.Connect().Filter(Observable.Return(true), static (include, _) => include).Select(static changes => changes.Count),
                        source.Add,
                        action => source.Edit(_ => action()),
                        scheduler,
                        source);
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    /// <param name="Emissions">The operator's notifications, as a count of items.</param>
    /// <param name="Write">Writes an item into the operator's source, taking the source's lock.</param>
    /// <param name="UnderSourceLock">Runs an action while holding the source's lock.</param>
    /// <param name="Scheduler">The operator's scheduler.</param>
    /// <param name="Owner">Owns the source.</param>
    /// <param name="Produce">Publishes an item from the producer thread; <paramref name="Write"/> when null.</param>
    private sealed record OperatorCase(IObservable<int> Emissions, Action<int> Write, Action<Action> UnderSourceLock, TestScheduler Scheduler, IDisposable Owner, Action<int>? Produce = null) : IDisposable
    {
        /// <summary>Publishes an expiring item and lets it expire, so every operator delivers at least once.</summary>
        public void Trigger(int value)
        {
            (Produce ?? Write)(value);
            Scheduler.AdvanceBy(Lifetime.Ticks);
        }

        public void Dispose() => Owner.Dispose();
    }

    /// <summary>
    /// A source with a lock that writers take, like an application lock around a subject. The producer
    /// that starts the test publishes without it, so the observer can need it while the disposer holds it.
    /// </summary>
    private sealed class LockedSource<T> : IObservable<T>
    {
        private readonly object _gate = new();
        private IObserver<T>? _observer;

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return Disposable.Create(() => _observer = null);
        }

        public void Push(T value) => _observer?.OnNext(value);

        public void Write(T value)
        {
            lock (_gate)
            {
                Push(value);
            }
        }

        public void UnderLock(Action action)
        {
            lock (_gate)
            {
                action();
            }
        }
    }
}
