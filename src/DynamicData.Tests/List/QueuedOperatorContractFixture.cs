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
    [Arguments("auto-refresh")]
    [Arguments("filter-on-observable")]
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
    [Arguments("auto-refresh")]
    [Arguments("filter-on-observable")]
    public async Task ConcurrentCrossPropertyChangesReleaseOperatorGates(string operation)
    {
        var firstCell = new Cell(1);
        var secondCell = new Cell(2);
        var firstRemoved = new Cell(5);
        var secondRemoved = new Cell(6);
        var firstList = new SourceList<Cell>();
        var secondList = new SourceList<Cell>();
        firstList.AddRange([firstCell, firstRemoved]);
        secondList.AddRange([secondCell, secondRemoved]);
        using var rendezvous = new Barrier(2);
        var firstEntered = 0;
        var secondEntered = 0;
        var firstUpdates = 0;
        var secondUpdates = 0;
        var armed = 0;

        // Each observer, on its first update after arming, waits until the other observer is running
        // and then changes a property of an item in the other list, which the other operator observes.
        var firstResults = PropertyPipeline(firstList.Connect(), operation)
            .Do(_ =>
            {
                Interlocked.Increment(ref firstUpdates);
                if (Volatile.Read(ref armed) != 0 && Interlocked.Exchange(ref firstEntered, 1) == 0)
                {
                    if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                    {
                        throw new TimeoutException("The other producer did not enter its observer.");
                    }

                    secondCell.Value = 20;
                }
            })
            .AsAggregator();
        var secondResults = PropertyPipeline(secondList.Connect(), operation)
            .Do(_ =>
            {
                Interlocked.Increment(ref secondUpdates);
                if (Volatile.Read(ref armed) != 0 && Interlocked.Exchange(ref secondEntered, 1) == 0)
                {
                    if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
                    {
                        throw new TimeoutException("The other producer did not enter its observer.");
                    }

                    firstCell.Value = 10;
                }
            })
            .AsAggregator();
        Volatile.Write(ref armed, 1);

        // Removals are delivered on each operator's source path, where the old monitor gate was held
        // while the observer changed the other list's item.
        var firstWriter = Task.Factory.StartNew(() => firstList.Remove(firstRemoved), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var secondWriter = Task.Factory.StartNew(() => secondList.Remove(secondRemoved), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // A deadlocked delivery is left undisposed: disposal can wait for it and hang the run.
        await Task.WhenAll(firstWriter, secondWriter).WaitAsync(TimeSpan.FromSeconds(15));

        using (firstResults)
        using (secondResults)
        using (firstList)
        using (secondList)
        {
            // Initial changeset, the removal, then the other observer's property change
            // (a refresh, or for the filter an inclusion of the changed item).
            await Assert.That(firstResults.Exception).IsNull();
            await Assert.That(secondResults.Exception).IsNull();
            await Assert.That(firstUpdates).IsEqualTo(3);
            await Assert.That(secondUpdates).IsEqualTo(3);
            await Assert.That(firstResults.Data.Items).IsEquivalentTo(new[] { firstCell });
            await Assert.That(secondResults.Data.Items).IsEquivalentTo(new[] { secondCell });
            await Assert.That(firstCell.Value).IsEqualTo(10);
            await Assert.That(secondCell.Value).IsEqualTo(20);
        }
    }

    private static IObservable<IChangeSet<Cell>> PropertyPipeline(IObservable<IChangeSet<Cell>> input, string operation) => operation switch
    {
        "auto-refresh" => input.AutoRefresh(static cell => cell.Value),
        "filter-on-observable" => input.FilterOnObservable(static cell => cell.WhenValueChanged(static c => c.Value).Select(static value => value > 2)),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

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
        "auto-refresh" => input.AutoRefreshOnObservable(static _ => Observable.Never<Unit>()).Select(changes => changes.Count),
        "filter-on-observable" => input.FilterOnObservable(static _ => Observable.Return(true)).Select(changes => changes.Count),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static ChangeSet<int> Add(int value) => new() { new(ListChangeReason.Add, value, 0) };

    private sealed class Cell(int value) : AbstractNotifyPropertyChanged
    {
        private int _value = value;

        public int Value
        {
            get => _value;
            set => SetAndRaise(ref _value, value);
        }
    }

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
