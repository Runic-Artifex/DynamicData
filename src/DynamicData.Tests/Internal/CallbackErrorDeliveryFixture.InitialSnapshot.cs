#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Internal;

/// <summary>
/// DD-044 (fork issues #36 and #37): a subscriber that throws at a value of the initial snapshot, delivered
/// inside <c>Subscribe</c>. The exception is thrown from <c>Subscribe</c>, is not delivered as <c>OnError</c>,
/// and ends the subscription: nothing else reaches the subscriber, and no input subscribed before the
/// exception keeps it reachable. Every pipeline catalogue of this fixture is covered, together with pipelines
/// whose other inputs are still live when the exception is thrown (a second source, or a parameter
/// observable that stays subscribed).
/// </summary>
public partial class CallbackErrorDeliveryFixture
{
    // These pipelines deliver nothing while they are subscribed.
    private static readonly HashSet<string> PipelinesWithoutInitialValue = ["SkipInitial"];

    public static IEnumerable<Func<string>> CacheLiveInputPipelines() =>
        new[] { "LeftJoin", "RightJoin", "FullJoin", "InnerJoin", "Or", "And", "Xor", "Except", "DynamicOr", "DynamicAnd", "DynamicXor", "DynamicExcept", "MergeChangeSets", "Sort", "Page", "Virtualise", "SortAndPage", "SortAndVirtualize", "Filter", "FilterPredicateState", "FilterReapply", "TransformForced", "BatchIf", "GroupWithRegrouper", "GroupWithImmutableStateWithRegrouper", "GroupDynamic", "GroupOnObservable", "QueryWhenChangedWithItemTrigger", "TransformOnObservable", "MergeMany", "AutoRefreshOnObservable", "TrueForAll", "TransformToTree", "MergeManyChangeSets", "Switch", "QueryWhenChangedWithLiveTrigger", "QueryWhenChangedWithItemTriggerLiveSource", "TrueForAny", "TrueForAllLiveSource", "GroupWithGroupSource", "GroupWithLiveGroupSource", "BindPaged", "BindPagedWithOptions", "BindVirtualized", "BindVirtualizedWithOptions", "TransformToTreeLiveSource" }
            .Select(name => (Func<string>)(() => name));

    public static IEnumerable<Func<string>> ListLiveInputPipelines() =>
        new[] { "Or", "And", "Xor", "Except", "DynamicOr", "MergeChangeSets", "Sort", "Page", "Virtualise", "Filter", "FilterPredicateState", "BufferIf", "GroupWithImmutableStateWithRegrouper", "MergeMany", "AutoRefreshOnObservable", "FilterOnObservable", "MergeManyChangeSets", "TransformMany", "Switch" }
            .Select(name => (Func<string>)(() => name));

    [Test]
    [MethodDataSource(nameof(CachePipelines))]
    public async Task CacheInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);

        await InitialSnapshotOutcome.Run(BuildCache(cache, pipeline), () => cache.AddOrUpdate(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(ListPipelines))]
    public async Task ListInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);

        await InitialSnapshotOutcome.Run(BuildList(list, pipeline), () => list.Add(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(CacheOperatorPipelines))]
    public async Task CacheOperatorInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);

        await InitialSnapshotOutcome.Run(BuildCacheOperator(cache, pipeline), () => cache.AddOrUpdate(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(ListOperatorPipelines))]
    public async Task ListOperatorInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);

        await InitialSnapshotOutcome.Run(BuildListOperator(list, pipeline), () => list.Add(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(CacheLiveInputPipelines))]
    public async Task CacheInitialSnapshotSubscriberExceptionReleasesLiveInputs(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var other = new SourceCache<int, int>(static value => value);
        using var signal = new Signal<int>();
        cache.AddOrUpdate(0);

        var outcome = InitialSnapshotOutcome.Run(BuildCacheWithLiveInput(cache, other, signal, pipeline), () => { cache.AddOrUpdate(1); other.AddOrUpdate(2); signal.OnNext(3); });

        await outcome.AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(ListLiveInputPipelines))]
    public async Task ListInitialSnapshotSubscriberExceptionReleasesLiveInputs(string pipeline)
    {
        using var list = new SourceList<int>();
        using var other = new SourceList<int>();
        using var signal = new Signal<int>();
        list.Add(0);

        var outcome = InitialSnapshotOutcome.Run(BuildListWithLiveInput(list, other, signal, pipeline), () => { list.Add(1); other.Add(2); signal.OnNext(3); });

        await outcome.AssertThrownFromSubscribe(pipeline);
    }

    // A parameter observable that emits while it is subscribed and stays subscribed to the signal.
    private static IObservable<T> LiveParameter<T>(T value, Signal<int> signal, Func<int, T> next) => Observable.Return(value).Concat(signal.Select(next));

    // The input that is subscribed first is the live one (the other source, or the parameter), so it is
    // attached when the subscriber throws at the initial changes of the cache.
    private static IObservable<object> BuildCacheWithLiveInput(SourceCache<int, int> cache, SourceCache<int, int> other, Signal<int> signal, string pipeline) => pipeline switch
    {
        "LeftJoin" => cache.Connect().LeftJoin(other.Connect(), static right => right, static (left, _) => left),
        "RightJoin" => other.Connect().RightJoin(cache.Connect(), static right => right, static (_, right) => right),
        "FullJoin" => other.Connect().FullJoin(cache.Connect(), static right => right, static (key, _, _) => key),
        "InnerJoin" => other.Connect().InnerJoin(cache.Connect(), static right => right, static (left, _) => left),
        "Or" => other.Connect().Or(cache.Connect()),
        "And" => new List<IObservable<IChangeSet<int, int>>> { other.Connect().Or(cache.Connect()), cache.Connect() }.And(),
        "Xor" => other.Connect().Xor(cache.Connect()),
        "Except" => cache.Connect().Except(other.Connect()),
        "DynamicOr" => Sources(other.Connect(), cache.Connect()).Or(),
        "DynamicAnd" => Sources(other.Connect().Or(cache.Connect()), cache.Connect()).And(),
        "DynamicXor" => Sources(other.Connect(), cache.Connect()).Xor(),
        "DynamicExcept" => Sources(cache.Connect(), other.Connect()).Except(),
        "MergeChangeSets" => new[] { other.Connect(), cache.Connect() }.MergeChangeSets(),
        "Sort" => cache.Connect().Sort(LiveParameter<IComparer<int>>(SortExpressionComparer<int>.Ascending(static value => value), signal, static _ => SortExpressionComparer<int>.Descending(static value => value))),
        "Page" => cache.Connect().Sort(SortExpressionComparer<int>.Ascending(static value => value)).Page(LiveParameter(new PageRequest(1, 10), signal, static size => new PageRequest(1, size + 1))),
        "Virtualise" => cache.Connect().Sort(SortExpressionComparer<int>.Ascending(static value => value)).Virtualise(LiveParameter<IVirtualRequest>(new VirtualRequest(0, 10), signal, static size => new VirtualRequest(0, size + 1))),
        "SortAndPage" => cache.Connect().SortAndPage(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter(new PageRequest(1, 10), signal, static size => new PageRequest(1, size + 1))),
        "SortAndVirtualize" => cache.Connect().SortAndVirtualize(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter<IVirtualRequest>(new VirtualRequest(0, 10), signal, static size => new VirtualRequest(0, size + 1))),
        "Filter" => cache.Connect().Filter(LiveParameter<Func<int, bool>>(static _ => true, signal, static _ => static _ => true)),
        "FilterPredicateState" => cache.Connect().Filter(LiveParameter(0, signal, static state => state), static (_, _) => true),
        "FilterReapply" => cache.Connect().Filter(Observable.Return<Func<int, bool>>(static _ => true), signal.Select(static _ => Unit.Default)),
        "TransformForced" => cache.Connect().Transform(static value => value, signal.Select(static _ => (Func<int, bool>)(static _ => true))),
        "BatchIf" => cache.Connect().BatchIf(LiveParameter(false, signal, static value => value > 0), false, (TimeSpan?)null),
        "GroupWithRegrouper" => cache.Connect().Group(static value => value % 2, signal.Select(static _ => Unit.Default)),
        "GroupWithImmutableStateWithRegrouper" => cache.Connect().GroupWithImmutableState(static value => value % 2, signal.Select(static _ => Unit.Default)),
        "GroupDynamic" => cache.Connect().Group(LiveParameter<Func<int, int, int>>(static (value, _) => value % 2, signal, static _ => static (value, _) => value % 3)),
        "GroupOnObservable" => cache.Connect().GroupOnObservable(_ => LiveParameter(0, signal, static group => group)),
        "QueryWhenChangedWithItemTrigger" => cache.Connect().QueryWhenChanged((int _) => signal.Select(static _ => Unit.Default)),
        "TransformOnObservable" => cache.Connect().TransformOnObservable(value => LiveParameter(value, signal, static next => next)),
        "MergeMany" => cache.Connect().MergeMany(value => LiveParameter(value, signal, static next => next)).Select(static value => (object)value),
        "AutoRefreshOnObservable" => cache.Connect().AutoRefreshOnObservable(_ => signal),
        "TrueForAll" => cache.Connect().TrueForAll(_ => LiveParameter(true, signal, static _ => true), static value => value).Select(static value => (object)value),
        "TransformToTree" => cache.Connect().Transform(static value => value.ToString()).TransformToTree(static _ => -1, LiveParameter<Func<Node<string, int>, bool>>(static _ => true, signal, static _ => static _ => true)),
        "MergeManyChangeSets" => other.Connect().Or(cache.Connect()).MergeManyChangeSets(value => SingleChange(value).Concat(signal.Select(next => (IChangeSet<int, int>)new ChangeSet<int, int> { new Change<int, int>(ChangeReason.Add, next + 100, next + 100) }))),
        "Switch" => LiveParameter(other.Connect(), signal, _ => other.Connect()).Select(source => source.Or(cache.Connect())).Switch(),
        "QueryWhenChangedWithLiveTrigger" => cache.Connect().QueryWhenChanged((int _) => LiveParameter(Unit.Default, signal, static _ => Unit.Default)),
        "QueryWhenChangedWithItemTriggerLiveSource" => other.Connect().Or(cache.Connect()).QueryWhenChanged((int _) => LiveParameter(Unit.Default, signal, static _ => Unit.Default)),
        "TrueForAny" => cache.Connect().TrueForAny(_ => LiveParameter(true, signal, static _ => true), static value => value).Select(static value => (object)value),
        "TrueForAllLiveSource" => other.Connect().Or(cache.Connect()).TrueForAll(_ => LiveParameter(true, signal, static _ => true), static value => value).Select(static value => (object)value),
        "GroupWithGroupSource" => cache.Connect().Group(static value => value % 2, other.Connect().Or(cache.Connect()).DistinctValues(static value => value % 2)),
        "GroupWithLiveGroupSource" => other.Connect().Or(cache.Connect()).Group(static value => value % 2, LiveParameter<IDistinctChangeSet<int>>(GroupKeys(0), signal, static key => GroupKeys(key))),
        "BindPaged" => cache.Connect().SortAndPage(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter(new PageRequest(1, 10), signal, static size => new PageRequest(1, size + 1))).Bind(new List<int>()),
        "BindPagedWithOptions" => cache.Connect().SortAndPage(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter(new PageRequest(1, 10), signal, static size => new PageRequest(1, size + 1))).Bind(new List<int>(), new SortAndBindOptions()),
        "BindVirtualized" => cache.Connect().SortAndVirtualize(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter<IVirtualRequest>(new VirtualRequest(0, 10), signal, static size => new VirtualRequest(0, size + 1))).Bind(new List<int>()),
        "BindVirtualizedWithOptions" => cache.Connect().SortAndVirtualize(SortExpressionComparer<int>.Ascending(static value => value), LiveParameter<IVirtualRequest>(new VirtualRequest(0, 10), signal, static size => new VirtualRequest(0, size + 1))).Bind(new List<int>(), new SortAndBindOptions()),
        "TransformToTreeLiveSource" => other.Connect().Or(cache.Connect()).Transform(static value => value.ToString()).TransformToTree(static _ => -1, LiveParameter<Func<Node<string, int>, bool>>(static _ => true, signal, static _ => static _ => true)),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private static IObservable<object> BuildListWithLiveInput(SourceList<int> list, SourceList<int> other, Signal<int> signal, string pipeline) => pipeline switch
    {
        "Or" => other.Connect().Or(list.Connect()),
        "And" => new List<IObservable<IChangeSet<int>>> { other.Connect().Or(list.Connect()), list.Connect() }.And(),
        "Xor" => other.Connect().Xor(list.Connect()),
        "Except" => list.Connect().Except(other.Connect()),
        "DynamicOr" => Sources(other.Connect(), list.Connect()).Or(),
        "MergeChangeSets" => new[] { other.Connect(), list.Connect() }.MergeChangeSets(),
        "Sort" => list.Connect().Sort(LiveParameter<IComparer<int>>(Comparer<int>.Default, signal, static _ => Comparer<int>.Default)),
        "Page" => list.Connect().Page(LiveParameter(new PageRequest(1, 10), signal, static size => new PageRequest(1, size + 1))),
        "Virtualise" => list.Connect().Virtualise(LiveParameter<IVirtualRequest>(new VirtualRequest(0, 10), signal, static size => new VirtualRequest(0, size + 1))),
        "Filter" => list.Connect().Filter(LiveParameter<Func<int, bool>>(static _ => true, signal, static _ => static _ => true)),
        "FilterPredicateState" => list.Connect().Filter(LiveParameter(true, signal, static _ => true), static (include, _) => include),
        "BufferIf" => list.Connect().BufferIf(LiveParameter(false, signal, static value => value > 0), new TestScheduler()),
        "GroupWithImmutableStateWithRegrouper" => list.Connect().GroupWithImmutableState(static value => value, signal.Select(static _ => Unit.Default)),
        "MergeMany" => list.Connect().MergeMany(value => LiveParameter(value, signal, static next => next)).Select(static value => (object)value),
        "AutoRefreshOnObservable" => list.Connect().AutoRefreshOnObservable(_ => signal),
        "FilterOnObservable" => list.Connect().FilterOnObservable(_ => LiveParameter(true, signal, static _ => true)),
        "MergeManyChangeSets" => other.Connect().Or(list.Connect()).MergeManyChangeSets(value => Observable.Return<IChangeSet<int>>(new ChangeSet<int>([new Change<int>(ListChangeReason.Add, value)])).Concat(signal.Select(next => (IChangeSet<int>)new ChangeSet<int>([new Change<int>(ListChangeReason.Add, next)])))),
        "TransformMany" => other.Connect().Or(list.Connect()).TransformMany(static value => new ObservableCollection<int> { value }),
        "Switch" => LiveParameter(other.Connect(), signal, _ => other.Connect()).Select(source => source.Or(list.Connect())).Switch(),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private static IDistinctChangeSet<int> GroupKeys(int key) => new DistinctChangeSet<int> { new Change<int, int>(ChangeReason.Add, key, key) };

    private static IObservableList<IObservable<T>> Sources<T>(IObservable<T> first, IObservable<T> second)
    {
        var sources = new SourceList<IObservable<T>>();
        sources.AddRange([first, second]);
        return sources;
    }

    private sealed record InitialSnapshotOutcome(Exception Expected, Exception? Thrown, Exception? Error, int Deliveries, bool IsSubscriberReachable, Exception? LaterThrown)
    {
        public static InitialSnapshotOutcome Run(IObservable<object> source, Action laterEdit)
        {
            var expected = new InvalidOperationException("subscriber");
            var (thrown, subscriber, reference) = Subscribe(source, expected);

            // Anything that still holds the subscriber is reachable from the inputs, which the pipeline and the
            // caller keep alive.
            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            var isReachable = reference.IsAlive;
            var laterThrown = CaptureThrown(laterEdit);
            GC.KeepAlive(source);

            return new InitialSnapshotOutcome(expected, thrown, subscriber.Error, subscriber.Deliveries, isReachable, laterThrown);
        }

        public async Task AssertThrownFromSubscribe(string pipeline)
        {
            if (PipelinesWithoutInitialValue.Contains(pipeline))
            {
                await Assert.That(Deliveries).IsEqualTo(0);
                await Assert.That(Thrown).IsNull();
                return;
            }

            await Assert.That(Thrown).IsSameReferenceAs(Expected);
            await Assert.That(Error).IsNull();
            await Assert.That(Deliveries).IsEqualTo(1);
            await Assert.That(IsSubscriberReachable).IsFalse();
            await Assert.That(LaterThrown).IsNull();
        }

        // Not inlined, so that nothing but the subscription can keep the subscriber alive.
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static (Exception? Thrown, Counts Subscriber, WeakReference Reference) Subscribe(IObservable<object> source, Exception expected)
        {
            var counts = new Counts();
            var subscriber = new ThrowingSubscriber(expected, counts);
            var reference = new WeakReference(subscriber);
            Exception? thrown = null;
            try
            {
                // Disposed when Subscribe returns, which it only does for a pipeline without an initial value.
                source.Subscribe(subscriber).Dispose();
            }
            catch (Exception ex)
            {
                thrown = ex;
            }

            return (thrown, counts, reference);
        }
    }

    private sealed class Counts
    {
        public int Deliveries { get; set; }

        public Exception? Error { get; set; }
    }

    private sealed class ThrowingSubscriber(Exception expected, Counts counts) : IObserver<object>
    {
        public void OnNext(object value)
        {
            counts.Deliveries++;
            throw expected;
        }

        public void OnError(Exception error) => counts.Error = error;

        public void OnCompleted()
        {
        }
    }
}
