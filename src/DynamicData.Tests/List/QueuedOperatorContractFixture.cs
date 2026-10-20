#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.List;

public class QueuedOperatorContractFixture
{
    [Test]
    [Arguments("sort")]
    [Arguments("buffer")]
    [Arguments("filter")]
    [Arguments("group")]
    [Arguments("immutable-group")]
    [Arguments("combine")]
    [Arguments("merge")]
    [Arguments("merge-many")]
    [Arguments("adapt")]
    [Arguments("dispose")]
    [Arguments("page")]
    [Arguments("to-observable-change-set")]
    [Arguments("filter-predicate-state")]
    public async Task ConcurrentCrossWritesReleaseOperatorGates(string operation)
    {
        var first = new DirectSource<IChangeSet<int>>();
        var second = new DirectSource<IChangeSet<int>>();
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

                second.Push(Add(3));
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

                first.Push(Add(4));
            }
        }, error => secondError = error);

        // Dedicated threads establish real simultaneous observer ownership. Both observers
        // then write to the opposing input, reproducing the ABBA window of monitor gates.
        var firstWriter = Task.Factory.StartNew(() => first.Push(Add(1)), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var secondWriter = Task.Factory.StartNew(() => second.Push(Add(2)), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

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
    [Arguments("expire-after")]
    [Arguments("expire-after-polling")]
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

    private const int NonExpiring = 100;

    private static readonly TimeSpan Lifetime = TimeSpan.FromMilliseconds(10);

    private static TimeSpan? ExpireBelowNonExpiring(int value) => value < NonExpiring ? Lifetime : null;

    private static ExpiringOperator ExpiringPipeline(string operation)
    {
        var scheduler = new TestScheduler();
        switch (operation)
        {
            case "expire-after":
            case "expire-after-polling":
                {
                    var source = new SourceList<int>();
                    var polling = operation == "expire-after-polling" ? Lifetime : (TimeSpan?)null;
                    return new(
                        source.ExpireAfter(ExpireBelowNonExpiring, polling, scheduler).Select(static items => items.Count()),
                        source.Add,
                        scheduler,
                        UpdatesPerRound: 1,
                        source);
                }

            case "to-observable-change-set":
                {
                    var source = new DirectSource<int>();
                    return new(
                        source.ToObservableChangeSet(ExpireBelowNonExpiring, scheduler).Select(static changes => changes.Count),
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

    private static IObservable<int> Pipeline(IObservable<IChangeSet<int>> input, string operation) => operation switch
    {
        "sort" => input.Sort(Comparer<int>.Default).Select(changes => changes.Count),
        "buffer" => input.BufferIf(Observable.Never<bool>(), scheduler: Scheduler.Immediate).Select(changes => changes.Count),
        "filter" => input.Filter(Observable.Return<Func<int, bool>>(static _ => true)).Select(changes => changes.Count),
        "group" => input.GroupOn(static value => value).Select(changes => changes.Count),
        "immutable-group" => input.GroupWithImmutableState(static value => value).Select(changes => changes.Count),
        "combine" => new[] { input }.Or().Select(changes => changes.Count),
        "merge" => Observable.Return(input).MergeChangeSets().Select(changes => changes.Count),
        "merge-many" => input.MergeMany(static value => Observable.Return(value)),
        "adapt" => input.Adapt(new NoopAdaptor()).Select(changes => changes.Count),
        "dispose" => input.DisposeMany().Select(changes => changes.Count),
        "page" => input.Page(Observable.Return<IPageRequest>(new PageRequest(1, 25))).Select(changes => changes.Count),

        // ToObservableChangeSet publishes an initial empty changeset on subscription.
        "to-observable-change-set" => input.Select(static changes => changes.First().Item.Current).ToObservableChangeSet().Skip(1).Select(changes => changes.Count),
        "filter-predicate-state" => input.Filter(Observable.Return(true), static (include, _) => include).Select(changes => changes.Count),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static ChangeSet<int> Add(int value) => new() { new(ListChangeReason.Add, value, 0) };

    private sealed class NoopAdaptor : IChangeSetAdaptor<int>
    {
        public void Adapt(IChangeSet<int> changes) { }
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
