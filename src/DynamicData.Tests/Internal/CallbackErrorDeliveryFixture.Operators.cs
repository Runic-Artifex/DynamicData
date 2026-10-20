using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Internal;

/// <summary>
/// DD-044 (fork issue #25): operators whose internal subscriptions were made through the static
/// <c>PrimitivesLinqExtensions.SubscribeSafe(source, onNext, ...)</c> in both flavours. A subscriber's
/// own exception is thrown to the producer and ends the subscription; an exception thrown by the
/// operator's own callbacks (predicates, selectors, comparers) is delivered as <c>OnError</c>.
/// </summary>
public partial class CallbackErrorDeliveryFixture
{
    public static IEnumerable<Func<string>> CacheOperatorPipelines() =>
        new[] { "FilterPredicateChanged", "FilterReapply", "FilterPredicateState", "MergeChangeSets", "MergeChangeSetsObservable", "GroupDynamic", "OfType", "AsyncDisposeMany", "ExpireAfter", "TransformOnObservable", "MergeManyChangeSets", "LockFreeCache" }
            .Select(name => (Func<string>)(() => name));

    public static IEnumerable<Func<string>> ListOperatorPipelines() =>
        new[] { "FilterPredicateState", "MergeManyChangeSets" }
            .Select(name => (Func<string>)(() => name));

    [Test]
    [MethodDataSource(nameof(CacheOperatorPipelines))]
    public async Task CacheOperatorSubscriberExceptionIsThrownToEditor(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);
        var outcome = SubscriberOutcome.Run(BuildCacheOperator(cache, pipeline), () => cache.AddOrUpdate(1), () => cache.AddOrUpdate(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    [MethodDataSource(nameof(ListOperatorPipelines))]
    public async Task ListOperatorSubscriberExceptionIsThrownToEditor(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);
        var outcome = SubscriberOutcome.Run(BuildListOperator(list, pipeline), () => list.Add(1), () => list.Add(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task FilterReapplySubscriberExceptionIsThrownToReapplyingCaller()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var reapply = new Signal<Unit>();
        var include = true;
        cache.AddOrUpdate(0);
        var outcome = SubscriberOutcome.Run(
            cache.Connect().Filter(Observable.Return<Func<int, bool>>(_ => include), reapply),
            () => { include = false; reapply.OnNext(Unit.Default); },
            () => { include = true; reapply.OnNext(Unit.Default); });

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task GroupDynamicRegroupSubscriberExceptionIsThrownToRegroupingCaller()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var regrouper = new Signal<Unit>();
        var offset = 0;
        cache.AddOrUpdate(0);
        var outcome = SubscriberOutcome.Run(
            cache.Connect().Group(Observable.Return<Func<int, int, int>>((value, _) => value + offset), regrouper),
            () => { offset = 1; regrouper.OnNext(Unit.Default); },
            () => { offset = 2; regrouper.OnNext(Unit.Default); });

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task CacheToObservableChangeSetSubscriberExceptionIsThrownToProducer()
    {
        using var source = new Signal<int>();
        var outcome = SubscriberOutcome.Run(source.ToObservableChangeSet(static value => value), () => source.OnNext(1), () => source.OnNext(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task ListToObservableChangeSetSubscriberExceptionIsThrownToProducer()
    {
        using var source = new Signal<int>();
        var outcome = SubscriberOutcome.Run(source.ToObservableChangeSet(), () => source.OnNext(1), () => source.OnNext(2));

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task CacheExpireAfterSubscriberExceptionIsThrownToScheduler()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var scheduler = new TestScheduler();
        var outcome = SubscriberOutcome.Run(
            cache.ExpireAfter(static _ => TimeSpan.FromTicks(10), scheduler: scheduler),
            () => { cache.AddOrUpdate(1); scheduler.AdvanceBy(10); },
            () => { cache.AddOrUpdate(2); scheduler.AdvanceBy(10); });

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task ListExpireAfterSubscriberExceptionIsThrownToScheduler()
    {
        using var list = new SourceList<int>();
        var scheduler = new TestScheduler();
        var outcome = SubscriberOutcome.Run(
            list.ExpireAfter(static _ => TimeSpan.FromTicks(10), scheduler: scheduler),
            () => { list.Add(1); scheduler.AdvanceBy(10); },
            () => { list.Add(2); scheduler.AdvanceBy(10); });

        await outcome.AssertThrownToEditor();
    }

    [Test]
    public async Task CacheFilterPredicateFailureOnSourceChangeIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Filter(Observable.Return<Func<int, bool>>(value => value == 1 ? throw error : true)), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheFilterPredicateFailureOnReapplyIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var reapply = new Signal<Unit>();
        var fail = false;
        cache.AddOrUpdate(1);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Filter(Observable.Return<Func<int, bool>>(_ => fail ? throw error : true), reapply), () => { fail = true; reapply.OnNext(Unit.Default); });

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    [Test]
    public async Task CacheFilterPredicateFailureOnPredicateChangeIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var predicates = new Signal<Func<int, bool>>();
        cache.AddOrUpdate(1);
        Exception? failure = null;
        var outcome = CallbackOutcome.Run(
            error =>
            {
                failure = error;
                return cache.Connect().Filter(predicates);
            },
            () => predicates.OnNext(_ => throw failure!));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListFilterPredicateFailureOnSourceChangeIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        var outcome = CallbackOutcome.Run(error => list.Connect().Filter(Observable.Return(0), (_, value) => value == 1 ? throw error : true), () => list.Add(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListFilterPredicateFailureOnPredicateStateChangeIsDeliveredAsOnError()
    {
        using var list = new SourceList<int>();
        using var predicateState = new Signal<int>();
        list.Add(1);
        var outcome = CallbackOutcome.Run(error => list.Connect().Filter(predicateState, (state, _) => state == 1 ? throw error : true), () => predicateState.OnNext(0), () => predicateState.OnNext(1));

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    [Test]
    public async Task CacheGroupDynamicSelectorFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Group(Observable.Return<Func<int, int, int>>((value, _) => value == 1 ? throw error : value)), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheGroupDynamicRegroupFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        using var regrouper = new Signal<Unit>();
        var fail = false;
        cache.AddOrUpdate(1);
        var outcome = CallbackOutcome.Run(error => cache.Connect().Group(Observable.Return<Func<int, int, int>>((value, _) => fail ? throw error : value), regrouper), () => { fail = true; regrouper.OnNext(Unit.Default); });

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    [Test]
    public async Task CacheMergeChangeSetsComparerFailureIsDeliveredAsOnError()
    {
        using var first = new SourceCache<int, int>(static value => value);
        using var second = new SourceCache<int, int>(static value => value);
        first.AddOrUpdate(1);
        var outcome = CallbackOutcome.Run(error => new[] { first.Connect(), second.Connect() }.MergeChangeSets(new ThrowingOrderComparer(error)), () => second.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError(expectedValues: 1);
    }

    [Test]
    public async Task CacheTransformOnObservableFactoryFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().TransformOnObservable(value => value == 1 ? throw error : Observable.Return(value)), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheExpireAfterTimeSelectorFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var outcome = CallbackOutcome.Run(error => cache.Connect().ExpireAfter(value => value == 1 ? throw error : null, new TestScheduler()), () => cache.AddOrUpdate(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task CacheToObservableChangeSetKeySelectorFailureIsDeliveredAsOnError()
    {
        using var source = new Signal<int>();
        var outcome = CallbackOutcome.Run(error => source.ToObservableChangeSet<int, int>(value => value == 1 ? throw error : value), () => source.OnNext(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task ListToObservableChangeSetExpireSelectorFailureIsDeliveredAsOnError()
    {
        using var source = new Signal<int>();
        var outcome = CallbackOutcome.Run(error => source.ToObservableChangeSet(value => value == 1 ? throw error : null, new TestScheduler()), () => source.OnNext(1));

        await outcome.AssertDeliveredAsOnError();
    }

    [Test]
    public async Task AsyncDisposeManyItemDisposalFailureIsDeliveredAsOnError()
    {
        using var cache = new SourceCache<ThrowingDisposable, int>(static item => item.Id);
        var outcome = CallbackOutcome.Run(
            error =>
            {
                cache.AddOrUpdate(new ThrowingDisposable(1, error));
                return cache.Connect().AsyncDisposeMany(static _ => { });
            },
            () => cache.RemoveKey(1));

        await outcome.AssertDeliveredAsOnError();
    }

    private static IObservable<object> BuildCacheOperator(SourceCache<int, int> cache, string pipeline) => pipeline switch
    {
        "FilterPredicateChanged" => cache.Connect().Filter(Observable.Return<Func<int, bool>>(static _ => true)),
        "FilterReapply" => cache.Connect().Filter(Observable.Return<Func<int, bool>>(static _ => true), Observable.Never<Unit>()),
        "FilterPredicateState" => cache.Connect().Filter(Observable.Return(0), static (_, _) => true),
        "MergeChangeSets" => new[] { cache.Connect(), Observable.Never<IChangeSet<int, int>>() }.MergeChangeSets(),
        "MergeChangeSetsObservable" => Observable.Return(cache.Connect()).MergeChangeSets(),
        "GroupDynamic" => cache.Connect().Group(Observable.Return<Func<int, int, int>>(static (value, _) => value % 2)),
        "OfType" => cache.Connect().OfType<int, int, int>(),
        "AsyncDisposeMany" => cache.Connect().AsyncDisposeMany(static _ => { }),
        "ExpireAfter" => cache.Connect().ExpireAfter(static _ => null),
        "TransformOnObservable" => cache.Connect().TransformOnObservable(static value => Observable.Return(value)),
        "MergeManyChangeSets" => cache.Connect().MergeManyChangeSets(static value => Observable.Return<IChangeSet<int, int>>(new ChangeSet<int, int>([new Change<int, int>(ChangeReason.Add, value, value)]))),
        "LockFreeCache" => cache.Connect().AsObservableCache(applyLocking: false).Connect(),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private static IObservable<object> BuildListOperator(SourceList<int> list, string pipeline) => pipeline switch
    {
        "FilterPredicateState" => list.Connect().Filter(Observable.Return(0), static (_, _) => true),
        "MergeManyChangeSets" => list.Connect().MergeManyChangeSets(static value => Observable.Return<IChangeSet<int>>(new ChangeSet<int>([new Change<int>(ListChangeReason.Add, value, 0)]))),
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, null),
    };

    private sealed class ThrowingOrderComparer(Exception error) : IComparer<int>
    {
        public int Compare(int x, int y) => throw error;
    }

    private sealed class ThrowingDisposable(int id, Exception error) : IDisposable
    {
        public int Id => id;

        public void Dispose() => throw error;
    }
}
