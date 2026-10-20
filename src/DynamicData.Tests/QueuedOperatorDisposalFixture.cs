#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
using DynamicData.Reactive.Binding;
#else
using DynamicData.Aggregation;
using DynamicData.Binding;
#endif

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
    [MethodDataSource(nameof(Operations))]
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
    [MethodDataSource(nameof(Operations))]
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
    [MethodDataSource(nameof(NonWaitingOperations))]
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

    private static readonly IComparer<int> Ascending = SortExpressionComparer<int>.Ascending(static value => value);

    /// <summary>
    /// Every operator that delivers through a <c>DeliveryQueue</c> or <c>SharedDeliveryQueue</c>, by name. Each
    /// publishes at least one notification per <see cref="OperatorCase.Trigger"/> and none on subscription.
    /// </summary>
    private static readonly Dictionary<string, Func<TestScheduler, OperatorCase>> Cases = new()
    {
        ["list-expire-after"] = static scheduler => ListCase(source => source.ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Select(static items => items.Count()), scheduler),
        ["cache-expire-after-source"] = static scheduler => CacheCase(source => source.ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Select(static items => items.Count()), scheduler),
        ["cache-expire-after-stream"] = static scheduler => CacheCase(source => source.Connect().ExpireAfter(ExpireBelowNonExpiring, null, scheduler).Counts(), scheduler),
        ["list-to-observable-change-set"] = static scheduler => LockedCase(source => source.ToObservableChangeSet().Counts(), scheduler),
        ["cache-to-observable-change-set"] = static scheduler => LockedCase(source => source.ToObservableChangeSet(static value => value).Counts(), scheduler),
        ["list-filter-predicate-state"] = static scheduler => ListCase(source => source.Connect().Filter(Observable.Return(true), static (include, _) => include).Counts(), scheduler),

        // Cache operators.
        ["source-cache-connect"] = static scheduler => CacheCase(source => source.Connect().Counts(), scheduler),
        ["cache-auto-refresh"] = static scheduler => CacheCase(source => source.Connect().AutoRefreshOnObservable(static _ => Observable.Never<Unit>()).Counts(), scheduler),
        ["cache-filter-on-observable"] = static scheduler => CacheCase(source => source.Connect().FilterOnObservable(static _ => Observable.Return(true)).Counts(), scheduler),
        ["cache-batch-if"] = static scheduler => CacheCase(source => source.Connect().BatchIf(Observable.Return(false), scheduler).Counts(), scheduler),
        ["cache-buffer-initial"] = static scheduler => CacheCase(source => source.Connect().BufferInitial(Lifetime / 2, scheduler).Counts(), scheduler),
        ["cache-or"] = static scheduler => CacheCase(source => source.Connect().Or(Observable.Never<IChangeSet<int, int>>()).Counts(), scheduler),
        ["cache-dynamic-or"] = static scheduler => CacheCase(source => DynamicSources(source.Connect()).Or().Counts(), scheduler),
        ["cache-full-join"] = static scheduler => CacheCase(source => source.Connect().FullJoin(Observable.Never<IChangeSet<int, int>>(), static right => right, static (key, _, _) => key).Counts(), scheduler),
        ["cache-inner-join"] = static scheduler => CacheCase(source => source.Connect().InnerJoin(source.Connect(), static right => right, static (left, _) => left).Counts(), scheduler),
        ["cache-left-join"] = static scheduler => CacheCase(source => source.Connect().LeftJoin(Observable.Never<IChangeSet<int, int>>(), static right => right, static (left, _) => left).Counts(), scheduler),
        ["cache-right-join"] = static scheduler => CacheCase(source => Observable.Never<IChangeSet<int, int>>().RightJoin(source.Connect(), static right => right, static (_, right) => right).Counts(), scheduler),
        ["cache-group"] = static scheduler => CacheCase(source => source.Connect().Group(static value => value % 2, Observable.Never<Unit>()).Counts(), scheduler),
        ["cache-group-dynamic"] = static scheduler => CacheCase(source => source.Connect().Group(Observable.Return<Func<int, int>>(static value => value % 2)).Counts(), scheduler),
        ["cache-group-immutable"] = static scheduler => CacheCase(source => source.Connect().GroupWithImmutableState(static value => value % 2).Counts(), scheduler),
        ["cache-group-specified"] = static scheduler => CacheCase(source => source.Connect().Group(static value => value % 2, source.Connect().DistinctValues(static value => value % 2)).Counts(), scheduler),
        ["cache-group-on-observable"] = static scheduler => CacheCase(source => source.Connect().GroupOnObservable(static value => Observable.Return(value % 2)).Counts(), scheduler),
        ["cache-merge-change-sets"] = static scheduler => CacheCase(source => Observable.Return(source.Connect()).MergeChangeSets().Counts(), scheduler),
        ["cache-merge-many"] = static scheduler => CacheCase(source => source.Connect().MergeMany(static value => Observable.Return(value)), scheduler),
        ["cache-merge-many-change-sets"] = static scheduler => CacheCase(source => source.Connect().MergeManyChangeSets(static value => SingleChange(value)).Counts(), scheduler),
        ["cache-transform-on-observable"] = static scheduler => CacheCase(source => source.Connect().TransformOnObservable(static value => Observable.Return(value)).Counts(), scheduler),
        ["cache-transform-many-async"] = static scheduler => CacheCase(source => source.Connect().TransformManyAsync(static value => Task.FromResult<IEnumerable<int>>([value]), static value => value).Counts(), scheduler),
        ["cache-sort"] = static scheduler => CacheCase(source => source.Connect().Sort(Observable.Return(Ascending)).Counts(), scheduler),
        ["cache-page"] = static scheduler => CacheCase(source => source.Connect().Sort(Ascending).Page(Observable.Return(new PageRequest(1, 10))).Counts(), scheduler),
        ["cache-virtualise"] = static scheduler => CacheCase(source => source.Connect().Sort(Ascending).Virtualise(Observable.Return<IVirtualRequest>(new VirtualRequest(0, 10))).Counts(), scheduler),
        ["cache-sort-and-page"] = static scheduler => CacheCase(source => source.Connect().SortAndPage(Ascending, Observable.Return(new PageRequest(1, 10))).Counts(), scheduler),
        ["cache-sort-and-virtualize"] = static scheduler => CacheCase(source => source.Connect().SortAndVirtualize(Ascending, Observable.Return(new VirtualRequest(0, 10))).Counts(), scheduler),
        ["cache-sort-and-bind"] = static scheduler => CacheCase(source => source.Connect().SortAndBind(out _, Observable.Return(Ascending)).Counts(), scheduler),
        ["cache-bind"] = static scheduler => CacheCase(source => source.Connect().Bind(new ObservableCollectionExtended<int>(), new ObservableCollectionAdaptor<int, int>()).Counts(), scheduler),
        ["cache-query-when-changed"] = static scheduler => CacheCase(source => source.Connect().QueryWhenChanged(static (int _) => Observable.Never<Unit>()).Select(static query => query.Count).Where(static count => count != 0), scheduler),
        ["cache-transform-async"] = static scheduler => CacheCase(source => source.Connect().TransformAsync(static value => Task.FromResult(value), Observable.Never<Func<int, int, bool>>()).Counts(), scheduler),
        ["cache-transform-many"] = static scheduler => CacheCase(source => source.Connect().TransformMany(static value => new ObservableCollection<int> { value }, static value => value).Counts(), scheduler),
        ["cache-transform-forced"] = static scheduler => CacheCase(source => source.Connect().Transform(static value => value, Observable.Never<Func<int, bool>>()).Counts(), scheduler),
        ["cache-transform-to-tree"] = static scheduler => CacheCase(source => source.Connect().Transform(static value => new Notifier { Value = value }).TransformToTree(static _ => 0).Counts(), scheduler),
        ["cache-to-observable-optional"] = static scheduler => CacheCase(source => source.Connect().ToObservableOptional(1, initialOptionalWhenMissing: true).Where(static value => value.HasValue).Select(static _ => 1), scheduler),
        ["cache-avg"] = static scheduler => CacheCase(source => source.Connect().Avg(static value => value).Where(static average => average != 0).Select(static _ => 1), scheduler),
        ["cache-on-item-removed"] = static scheduler => CacheCase(source => source.Connect().OnItemRemoved(static _ => { }).Counts(), scheduler),
        ["cache-async-dispose-many"] = static scheduler => CacheCase(source => source.Connect().AsyncDisposeMany(static _ => { }).Counts(), scheduler),
        ["cache-switch"] = static scheduler => CacheCase(source => Observable.Return(source.Connect()).Switch().Counts(), scheduler),

        // List operators.
        ["source-list-connect"] = static scheduler => ListCase(source => source.Connect().Counts(), scheduler),
        ["list-auto-refresh"] = static scheduler => ListCase(source => source.Connect().AutoRefreshOnObservable(static _ => Observable.Never<Unit>()).Counts(), scheduler),
        ["list-filter-on-observable"] = static scheduler => ListCase(source => source.Connect().FilterOnObservable(static _ => Observable.Return(true)).Counts(), scheduler),
        ["list-filter-dynamic"] = static scheduler => ListCase(source => source.Connect().Filter(Observable.Return<Func<int, bool>>(static _ => true)).Counts(), scheduler),
        ["list-buffer-if"] = static scheduler => ListCase(source => source.Connect().BufferIf(Observable.Return(false), scheduler).Counts(), scheduler),
        ["list-buffer-initial"] = static scheduler => ListCase(source => source.Connect().BufferInitial(Lifetime / 2, scheduler).Counts(), scheduler),
        ["list-or"] = static scheduler => ListCase(source => source.Connect().Or(Observable.Never<IChangeSet<int>>()).Counts(), scheduler),
        ["list-dynamic-or"] = static scheduler => ListCase(source => DynamicSources(source.Connect()).Or().Counts(), scheduler),
        ["list-group-on"] = static scheduler => ListCase(source => source.Connect().GroupOn(static value => value % 2).Counts(), scheduler),
        ["list-group-immutable"] = static scheduler => ListCase(source => source.Connect().GroupWithImmutableState(static value => value % 2).Counts(), scheduler),
        ["list-limit-size-to"] = static scheduler => ListCase(source => source.LimitSizeTo(1, scheduler).Select(static removed => removed.Count()), scheduler, initial: 0),
        ["list-merge-change-sets"] = static scheduler => ListCase(source => Observable.Return(source.Connect()).MergeChangeSets().Counts(), scheduler),
        ["list-merge-many"] = static scheduler => ListCase(source => source.Connect().MergeMany(static value => Observable.Return(value)), scheduler),
        ["list-merge-many-change-sets"] = static scheduler => ListCase(source => source.Connect().MergeManyChangeSets(static value => Observable.Return<IChangeSet<int>>(new ChangeSet<int>([new Change<int>(ListChangeReason.Add, value)]))).Counts(), scheduler),
        ["list-merge-many-cache-change-sets"] = static scheduler => ListCase(source => source.Connect().MergeManyChangeSets(static value => SingleChange(value)).Counts(), scheduler),
        ["list-page"] = static scheduler => ListCase(source => source.Connect().Page(Observable.Return(new PageRequest(1, 10))).Counts(), scheduler),
        ["list-virtualise"] = static scheduler => ListCase(source => source.Connect().Virtualise(Observable.Return<IVirtualRequest>(new VirtualRequest(0, 10))).Counts(), scheduler),
        ["list-sort"] = static scheduler => ListCase(source => source.Connect().Sort(Ascending).Counts(), scheduler),
        ["list-transform-many"] = static scheduler => ListCase(source => source.Connect().TransformMany(static value => new ObservableCollection<int> { value }).Counts(), scheduler),
        ["list-bind"] = static scheduler => ListCase(source => source.Connect().Bind(new ObservableCollectionExtended<int>()).Counts(), scheduler),
        ["list-dispose-many"] = static scheduler => ListCase(source => source.Connect().DisposeMany().Counts(), scheduler),
        ["list-switch"] = static scheduler => ListCase(source => Observable.Return(source.Connect()).Switch().Counts(), scheduler),

        // Property observation.
        ["when-value-changed"] = static scheduler => NotifierCase(static root => root.WhenValueChanged(static value => value.Value, notifyOnInitialValue: false), scheduler),
        ["when-value-changed-chain"] = static scheduler => NotifierCase(static root => root.WhenValueChanged(static value => value.Child!.Value, notifyOnInitialValue: false), scheduler),
    };

    /// <summary>
    /// Operators whose disposal deliberately waits for a delivery in progress on another thread, so that their
    /// teardown (disposing the items they track) never runs while that delivery still uses them. Disposing one
    /// while holding a lock its observer needs still deadlocks.
    /// </summary>
    private static readonly HashSet<string> WaitingOperations =
    [
        "list-dispose-many",
        "list-group-on", // Disposes its groups through list DisposeMany.
        "cache-on-item-removed",
        "cache-async-dispose-many",
    ];

    public static IEnumerable<string> Operations() => Cases.Keys;

    public static IEnumerable<string> NonWaitingOperations() => Cases.Keys.Where(static operation => !WaitingOperations.Contains(operation));

    private static TimeSpan? ExpireBelowNonExpiring(int value) => value < NonExpiring ? Lifetime : null;

    private static OperatorCase CreateCase(string operation)
        => Cases.TryGetValue(operation, out var create) ? create(new TestScheduler()) : throw new ArgumentOutOfRangeException(nameof(operation));

    private static OperatorCase CacheCase(Func<SourceCache<int, int>, IObservable<int>> emissions, TestScheduler scheduler)
    {
        var source = new SourceCache<int, int>(static value => value);
        return new(emissions(source), source.AddOrUpdate, action => source.Edit(_ => action()), scheduler, source);
    }

    private static OperatorCase ListCase(Func<SourceList<int>, IObservable<int>> emissions, TestScheduler scheduler, int? initial = null)
    {
        var source = new SourceList<int>();
        if (initial is { } value)
        {
            source.Add(value);
        }

        return new(emissions(source), source.Add, action => source.Edit(_ => action()), scheduler, source);
    }

    private static OperatorCase LockedCase(Func<LockedSource<int>, IObservable<int>> emissions, TestScheduler scheduler)
    {
        var source = new LockedSource<int>();
        return new(emissions(source), source.Write, source.UnderLock, scheduler, Disposable.Empty, source.Push);
    }

    private static OperatorCase NotifierCase(Func<Notifier, IObservable<int>> emissions, TestScheduler scheduler)
    {
        var root = new Notifier { Child = new Notifier() };
        void Write(Notifier target, int value)
        {
            target.Value = value;
            root.Child!.Value = value;
        }

        return new(
            emissions(root),
            value =>
            {
                lock (root)
                {
                    Write(root, value);
                }
            },
            action =>
            {
                lock (root)
                {
                    action();
                }
            },
            scheduler,
            Disposable.Empty,
            value => Write(root, value));
    }

    private static IObservable<IChangeSet<int, int>> SingleChange(int value)
        => Observable.Return<IChangeSet<int, int>>(new ChangeSet<int, int> { new Change<int, int>(ChangeReason.Add, value, value) });

    private static IObservableList<IObservable<T>> DynamicSources<T>(IObservable<T> source)
    {
        var sources = new SourceList<IObservable<T>>();
        sources.Add(source);
        return sources;
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

    /// <summary>An object with a notifying property and a child, for property observation.</summary>
    private sealed class Notifier : INotifyPropertyChanged
    {
        private Notifier? _child;
        private int _value;

        public event PropertyChangedEventHandler? PropertyChanged;

        public Notifier? Child
        {
            get => _child;
            set
            {
                _child = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }

        public int Value
        {
            get => _value;
            set
            {
                _value = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
            }
        }
    }
}

internal static class QueuedOperatorDisposalExtensions
{
    /// <summary>The size of each non-empty changeset.</summary>
    public static IObservable<int> Counts<TChangeSet>(this IObservable<TChangeSet> source)
        where TChangeSet : IChangeSet
        => source.Select(static changes => changes.Count).Where(static count => count != 0);
}
