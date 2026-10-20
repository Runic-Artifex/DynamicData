#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
using DynamicData.Reactive.Binding;
#else
using DynamicData.Aggregation;
using DynamicData.Binding;
#endif
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Internal;

/// <summary>
/// DD-044 (fork issue #19): where callback and subscriber exceptions go, asserted identically in
/// both flavours. The System.Reactive flavour is the reference:
/// <list type="bullet">
/// <item>an exception thrown by a subscriber's own <c>OnNext</c> is thrown to the code making the
/// edit, is not delivered to the subscriber's <c>OnError</c>, and ends the subscription;</item>
/// <item>an exception thrown by an operator callback (<c>Do</c>-based callbacks such as
/// <c>OnItemAdded</c> or <c>ForEachChange</c>, comparers, result selectors) is delivered as
/// <c>OnError</c>; without an error handler it is thrown to the editor.</item>
/// </list>
/// ReactiveUI.Primitives' <c>SubscribeSafe</c>, <c>Do</c>, <c>DistinctUntilChanged</c> and
/// <c>CombineLatest</c> previously converted, threw or lost these exceptions in the Primitives flavour.
/// </summary>
public partial class CallbackErrorDeliveryFixture
{
    public static IEnumerable<Func<string>> ListPipelines() =>
        new[] { "Connect", "ConnectWithPredicate", "Transform", "Filter", "Sort", "DeferUntilLoaded", "SkipInitial", "QueryWhenChanged", "Sum", "IsEmpty", "CountChanged", "SortWithComparerChanges", "Page", "Virtualise", "FilterPredicateChanges", "BufferIf", "AutoRefreshOnObservable", "AutoRefreshOnObservableSynchronous", "FilterOnObservable", "Or", "DynamicOr", "MergeChangeSets", "MergeMany", "MergeManyChangeSets", "TransformMany", "GroupWithImmutableStateWithRegrouper", "FilterPredicateState" }
            .Select(name => (Func<string>)(() => name));

    public static IEnumerable<Func<string>> CachePipelines() =>
        new[] { "Connect", "Transform", "Filter", "Sort", "ToCollection", "Group", "CountChanged", "SortWithComparerChanges", "Page", "Virtualise", "SortAndPage", "SortAndVirtualize", "TransformForced", "TransformAsyncForced", "BatchIf", "GroupWithRegrouper", "GroupWithImmutableStateWithRegrouper", "InnerJoin", "LeftJoin", "RightJoin", "FullJoin", "DynamicOr", "MergeMany", "AutoRefreshOnObservable", "AutoRefreshOnObservableSynchronous", "FilterOnObservable", "MergeManyChangeSets", "MergeManyChangeSetsWithSourceComparer", "MergeManyListChangeSets", "TransformOnObservable", "GroupOnObservable", "TransformManyAsync", "TransformMany", "QueryWhenChangedWithItemTrigger", "TransformToTree" }
            .Select(name => (Func<string>)(() => name));

    [Test]
    [MethodDataSource(nameof(ListPipelines))]
    public async Task ListSubscriberExceptionIsThrownToEditor(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);
        var outcome = SubscriberOutcome.Run(BuildList(list, pipeline), () => list.Add(1), () => list.Add(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    [MethodDataSource(nameof(CachePipelines))]
    public async Task CacheSubscriberExceptionIsThrownToEditor(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);
        var outcome = SubscriberOutcome.Run(BuildCache(cache, pipeline), () => cache.AddOrUpdate(1), () => cache.AddOrUpdate(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task ListWhenValueChangedInitialValueSubscriberExceptionIsThrownToEditor()
    {
        using var list = new SourceList<Person>();
        list.Add(new Person());
        var outcome = SubscriberOutcome.Run(list.Connect().WhenValueChanged(static person => person.Age).Select(static age => (object)age), () => list.Add(new Person()), () => list.Add(new Person()));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task CacheWhenValueChangedInitialValueSubscriberExceptionIsThrownToEditor()
    {
        using var cache = new SourceCache<Person, string>(static person => person.Name);
        cache.AddOrUpdate(new Person { Name = "0" });
        var outcome = SubscriberOutcome.Run(cache.Connect().WhenValueChanged(static person => person.Age).Select(static age => (object)age), () => cache.AddOrUpdate(new Person { Name = "1" }), () => cache.AddOrUpdate(new Person { Name = "2" }));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task SubscriberExceptionOnInitialSnapshotIsThrownFromSubscribe()
    {
        using var list = new SourceList<int>();
        list.Add(0);
        var expected = new InvalidOperationException("subscriber");
        Exception? error = null;
        Exception? thrown = null;

        try
        {
            list.Connect().Transform(static value => value).Subscribe(_ => throw expected, ex => error = ex);
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        await Assert.That(thrown).IsSameReferenceAs(expected);
        await Assert.That(error).IsNull();
    }

    [Test]
    public async Task ListOnItemAddedFailureIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        var outcome = CallbackOutcome.Run(error => list.Connect().OnItemAdded(_ => throw error), () => list.Add(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListOnItemAddedAfterTransformFailureIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        var outcome = CallbackOutcome.Run(error => list.Connect().Transform(static value => value).OnItemAdded(_ => throw error), () => list.Add(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListForEachChangeAfterTransformFailureIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        var outcome = CallbackOutcome.Run(error => list.Connect().Transform(static value => value).ForEachChange(_ => throw error), () => list.Add(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListForEachItemChangeAfterFilterFailureIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        var outcome = CallbackOutcome.Run(error => list.Connect().Filter(static _ => true).ForEachItemChange(_ => throw error), () => list.Add(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListOnItemRemovedAfterTransformFailureIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        list.Add(1);
        var outcome = CallbackOutcome.Run(error => list.Connect().Transform(static value => value).OnItemRemoved(_ => throw error, invokeOnUnsubscribe: false), () => list.Remove(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListOnItemAddedAfterTransformFailureWithoutErrorHandlerIsThrownToEditor()
    {
        using var list = new SourceList<int>();
        var expected = new InvalidOperationException("callback");
        using var subscription = list.Connect().Transform(static value => value).OnItemAdded(_ => throw expected).Subscribe(static _ => { });

        var thrown = CaptureThrown(() => list.Add(1));

        await Assert.That(thrown).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task CacheOnItemAddedAfterTransformFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Transform(static value => value).OnItemAdded(_ => throw error), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheForEachChangeAfterFilterFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Filter(static _ => true).ForEachChange(_ => throw error), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheOnItemRemovedAfterTransformFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(1);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Transform(static value => value).OnItemRemoved(_ => throw error, invokeOnUnsubscribe: false), () => cache.RemoveKey(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheOnItemAddedAfterTransformFailureWithoutErrorHandlerIsThrownToEditor()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var expected = new InvalidOperationException("callback");
        using var subscription = cache.Connect().Transform(static value => value).OnItemAdded(_ => throw expected).Subscribe(static _ => { });

        var thrown = CaptureThrown(() => cache.AddOrUpdate(1));

        await Assert.That(thrown).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task DoFailureIsDeliveredAsOnErrorAndUnsubscribes()
    {
        // Binds to the fork's internal Do in the Primitives flavour (DD-044), Observable.Do otherwise.
        using var source = new Signal<int>();
        var outcome = CallbackOutcome.Run(error => source.Do(value => { if (value == 2) throw error; }), () => source.OnNext(1), () => source.OnNext(2), () => source.OnNext(3));

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task DoCompletionCallbackFailureIsDeliveredAsOnError()
    {
        using var source = new Signal<int>();
        var outcome = CallbackOutcome.Run(error => source.Do(static _ => { }, () => throw error), source.OnCompleted);

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task DistinctUntilChangedComparerFailureIsDeliveredAsOnErrorAndUnsubscribes()
    {
        using var source = new Signal<int>();
        var outcome = CallbackOutcome.Run(error => source.DistinctUntilChanged(new ThrowingComparer(error)), () => source.OnNext(1), () => source.OnNext(2), () => source.OnNext(3));

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task WhenChangedResultSelectorFailureIsDeliveredAsOnError()
    {
        var person = new Person();
        var outcome = CallbackOutcome.Run(
            error => person.WhenChanged(static p => p.Age, static p => p.Name, (_, age, _) => age == 1 ? throw error : age),
            () => person.Age = 1,
            () => person.Age = 2);

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    [Test]
    public async Task TrueForAllConditionFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<Person, string>(static p => p.Name);
        var person = new Person { Name = "a" };
        var outcome = CallbackOutcome.Run(
            error => cache.Connect().TrueForAll(static p => p.WhenValueChanged(static x => x.Age), age => age == 1 ? throw error : true),
            () => cache.AddOrUpdate(person),
            () => person.Age = 1,
            () => person.Age = 2);

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    private static IObservable<object> BuildList(SourceList<int> list, string pipeline) => pipeline switch
    {
        "Connect" => list.Connect(),
        "ConnectWithPredicate" => list.Connect(static _ => true),
        "Transform" => list.Connect().Transform(static value => value),
        "Filter" => list.Connect().Filter(static _ => true),
        "Sort" => list.Connect().Sort(Comparer<int>.Default),
        "DeferUntilLoaded" => list.Connect().DeferUntilLoaded(),
        "SkipInitial" => list.Connect().SkipInitial(),
        "QueryWhenChanged" => list.Connect().QueryWhenChanged(),
        "Sum" => list.Connect().Sum(static value => value).Select(static value => (object)value),
        "IsEmpty" => list.Connect().IsEmpty().Select(static value => (object)value),
        "CountChanged" => list.CountChanged.Select(static value => (object)value),
        "SortWithComparerChanges" => list.Connect().Sort(Observable.Return<IComparer<int>>(Comparer<int>.Default)),
        "Page" => list.Connect().Page(Observable.Return(new PageRequest(1, 10))),
        "Virtualise" => list.Connect().Virtualise(Observable.Return<IVirtualRequest>(new VirtualRequest(0, 10))),
        "FilterPredicateChanges" => list.Connect().Filter(Observable.Return<Func<int, bool>>(static _ => true)),
        "BufferIf" => list.Connect().BufferIf(Observable.Return(false), new TestScheduler()),
        "AutoRefreshOnObservable" => list.Connect().AutoRefreshOnObservable(static _ => Observable.Never<Unit>()),
        "AutoRefreshOnObservableSynchronous" => list.Connect().AutoRefreshOnObservable(static _ => Observable.Return(Unit.Default)),
        "FilterOnObservable" => list.Connect().FilterOnObservable(static _ => Observable.Return(true)),
        "Or" => list.Connect().Or(Observable.Never<IChangeSet<int>>()),
        "DynamicOr" => Sources(list.Connect()).Or(),
        "MergeChangeSets" => new[] { list.Connect(), Observable.Never<IChangeSet<int>>() }.MergeChangeSets(),
        "MergeMany" => list.Connect().MergeMany(static value => Observable.Return(value)).Select(static value => (object)value),
        "MergeManyChangeSets" => list.Connect().MergeManyChangeSets(static value => Observable.Return<IChangeSet<int>>(new ChangeSet<int>([new Change<int>(ListChangeReason.Add, value)]))),
        "TransformMany" => list.Connect().TransformMany(static value => new ObservableCollection<int> { value }),
        "GroupWithImmutableStateWithRegrouper" => list.Connect().GroupWithImmutableState(static value => value, Observable.Never<Unit>()),
        "FilterPredicateState" => list.Connect().Filter(Observable.Return(true), static (include, _) => include),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private static IObservable<object> BuildCache(SourceCache<int, int> cache, string pipeline) => pipeline switch
    {
        "Connect" => cache.Connect(),
        "Transform" => cache.Connect().Transform(static value => value),
        "Filter" => cache.Connect().Filter(static _ => true),
        "Sort" => cache.Connect().Sort(SortExpressionComparer<int>.Ascending(static value => value)),
        "ToCollection" => cache.Connect().ToCollection(),
        "Group" => cache.Connect().Group(static value => value % 2),
        "CountChanged" => cache.CountChanged.Select(static value => (object)value),
        "SortWithComparerChanges" => cache.Connect().Sort(Observable.Return<IComparer<int>>(SortExpressionComparer<int>.Ascending(static value => value))),
        "Page" => cache.Connect().Sort(SortExpressionComparer<int>.Ascending(static value => value)).Page(Observable.Return(new PageRequest(1, 10))),
        "Virtualise" => cache.Connect().Sort(SortExpressionComparer<int>.Ascending(static value => value)).Virtualise(Observable.Return<IVirtualRequest>(new VirtualRequest(0, 10))),
        "SortAndPage" => cache.Connect().SortAndPage(SortExpressionComparer<int>.Ascending(static value => value), Observable.Return(new PageRequest(1, 10))),
        "SortAndVirtualize" => cache.Connect().SortAndVirtualize(SortExpressionComparer<int>.Ascending(static value => value), Observable.Return<IVirtualRequest>(new VirtualRequest(0, 10))),
        "TransformForced" => cache.Connect().Transform(static value => value, Observable.Never<Func<int, bool>>()),
        "TransformAsyncForced" => cache.Connect().TransformAsync(static value => Task.FromResult(value), Observable.Never<Func<int, int, bool>>()),
        "BatchIf" => cache.Connect().BatchIf(Observable.Return(false), false, (TimeSpan?)null),
        "GroupWithRegrouper" => cache.Connect().Group(static value => value % 2, Observable.Never<Unit>()),
        "GroupWithImmutableStateWithRegrouper" => cache.Connect().GroupWithImmutableState(static value => value % 2, Observable.Never<Unit>()),
        "InnerJoin" => cache.Connect().InnerJoin(cache.Connect(), static right => right, static (left, _) => left),
        "LeftJoin" => cache.Connect().LeftJoin(Observable.Never<IChangeSet<int, int>>(), static right => right, static (left, _) => left),
        "RightJoin" => Observable.Never<IChangeSet<int, int>>().RightJoin(cache.Connect(), static right => right, static (_, right) => right),
        "FullJoin" => cache.Connect().FullJoin(Observable.Never<IChangeSet<int, int>>(), static right => right, static (key, _, _) => key),
        "DynamicOr" => Sources(cache.Connect()).Or(),
        "MergeMany" => cache.Connect().MergeMany(static value => Observable.Return(value)).Select(static value => (object)value),
        "AutoRefreshOnObservable" => cache.Connect().AutoRefreshOnObservable(static _ => Observable.Never<Unit>()),
        "AutoRefreshOnObservableSynchronous" => cache.Connect().AutoRefreshOnObservable(static _ => Observable.Return(Unit.Default)),
        "FilterOnObservable" => cache.Connect().FilterOnObservable(static _ => Observable.Return(true)),
        "MergeManyChangeSets" => cache.Connect().MergeManyChangeSets(static value => SingleChange(value)),
        "MergeManyChangeSetsWithSourceComparer" => cache.Connect().MergeManyChangeSets(static value => SingleChange(value), sourceComparer: Comparer<int>.Default),
        "MergeManyListChangeSets" => cache.Connect().MergeManyChangeSets(static value => Observable.Return<IChangeSet<int>>(new ChangeSet<int>([new Change<int>(ListChangeReason.Add, value)]))),
        "TransformOnObservable" => cache.Connect().TransformOnObservable(static value => Observable.Return(value)),
        "GroupOnObservable" => cache.Connect().GroupOnObservable(static value => Observable.Return(value)),
        "TransformManyAsync" => cache.Connect().TransformManyAsync(static value => Task.FromResult<IEnumerable<int>>([value]), static value => value),
        "TransformMany" => cache.Connect().TransformMany(static value => new ObservableCollection<int> { value }, static value => value),
        "QueryWhenChangedWithItemTrigger" => cache.Connect().QueryWhenChanged(static (int _) => Observable.Never<Unit>()),
        "TransformToTree" => cache.Connect().Transform(static value => value.ToString()).TransformToTree(static _ => -1),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private static IObservable<IChangeSet<int, int>> SingleChange(int value)
        => Observable.Return<IChangeSet<int, int>>(new ChangeSet<int, int> { new Change<int, int>(ChangeReason.Add, value, value) });

    private static IObservableList<IObservable<T>> Sources<T>(IObservable<T> source)
    {
        var sources = new SourceList<IObservable<T>>();
        sources.Add(source);
        return sources;
    }

    private static Exception? CaptureThrown(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private sealed record SubscriberOutcome(Exception Expected, Exception? Thrown, Exception? Error, int DeliveriesAfterFailure, Exception? LaterThrown)
    {
        public static SubscriberOutcome Run(IObservable<object> source, Action failingEdit, Action laterEdit)
        {
            var expected = new InvalidOperationException("subscriber");
            var armed = false;
            var deliveriesAfterFailure = 0;
            Exception? error = null;

            using var subscription = source.Subscribe(
                _ =>
                {
                    if (armed)
                    {
                        armed = false;
                        throw expected;
                    }

                    deliveriesAfterFailure++;
                },
                ex => error = ex);

            deliveriesAfterFailure = 0;
            armed = true;
            var thrown = CaptureThrown(failingEdit);
            armed = false;
            var laterThrown = CaptureThrown(laterEdit);

            return new SubscriberOutcome(expected, thrown, error, deliveriesAfterFailure, laterThrown);
        }

        public async Task AssertThrownToEditor()
        {
            await Assert.That(Thrown).IsSameReferenceAs(Expected);
            await Assert.That(Error).IsNull();
            await Assert.That(DeliveriesAfterFailure).IsEqualTo(0);
            await Assert.That(LaterThrown).IsNull();
        }
    }

    private sealed record CallbackOutcome(Exception Expected, Exception? Thrown, Exception? Error, int Values)
    {
        public static CallbackOutcome Run<T>(Func<Exception, IObservable<T>> build, params Action[] actions)
        {
            var expected = new InvalidOperationException("callback");
            var values = 0;
            Exception? error = null;
            Exception? thrown = null;

            using (build(expected).Subscribe(_ => values++, ex => error = ex))
            {
                foreach (var action in actions)
                {
                    thrown ??= CaptureThrown(action);
                }
            }

            return new CallbackOutcome(expected, thrown, error, values);
        }

        public async Task AssertDeliveredAsOnError(int? expectedValues = null)
        {
            await Assert.That(Error).IsSameReferenceAs(Expected);
            await Assert.That(Thrown).IsNull();
            if (expectedValues is { } count)
            {
                await Assert.That(Values).IsEqualTo(count);
            }
        }
    }

    private sealed class ThrowingComparer(Exception error) : IEqualityComparer<int>
    {
        // The failure is the point of this comparer.
#pragma warning disable CA1065
        public bool Equals(int x, int y) => throw error;
#pragma warning restore CA1065

        public int GetHashCode(int obj) => obj;
    }

    private sealed class Person : AbstractNotifyPropertyChanged
    {
        private int _age;

        public int Age
        {
            get => _age;
            set => SetAndRaise(ref _age, value);
        }

        public string Name { get; init; } = "person";
    }
}
