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
        using var firstSubscription = Pipeline(first, operation).Subscribe(_ =>
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
        using var secondSubscription = Pipeline(second, operation).Subscribe(_ =>
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
        await Task.WhenAll(firstWriter, secondWriter).WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.That(firstError).IsNull();
        await Assert.That(secondError).IsNull();
        await Assert.That(firstUpdates).IsEqualTo(2);
        await Assert.That(secondUpdates).IsEqualTo(2);
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
