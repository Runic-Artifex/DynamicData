namespace DynamicData.Tests.List;

public sealed class FilterObservableInitialInclusionFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SynchronouslyFalsePredicate_NeverReachesDownstreamTransform(bool cache)
    {
        var transformed = new List<int>();
        if (cache)
        {
            using var source = new SourceCache<int, int>(static value => value);
            using var subscription = source.Connect().FilterOnObservable(static _ => Observable.Return(false))
                .Transform(value => { transformed.Add(value); return value; }).RecordCacheItems(out var results);
            source.AddOrUpdate(1);
            await Assert.That(results.Error).IsNull();
            await Assert.That(results.RecordedItemsByKey).IsEmpty();
        }
        else
        {
            using var source = new SourceList<int>();
            using var subscription = source.Connect().FilterOnObservable(static _ => Observable.Return(false))
                .Transform(value => { transformed.Add(value); return value; }).RecordListItems(out var results);
            source.Add(1);
            await Assert.That(results.Error).IsNull();
            await Assert.That(results.RecordedItems).IsEmpty();
        }

        await Assert.That(transformed).IsEmpty().Because("initially excluded rows must not transiently run an expensive transform");
    }

    [Test]
    public async Task SynchronousPredicateBurst_CommitsFinalInitialValue()
    {
        using var source = new SourceList<int>();
        var transforms = 0;
        using var subscription = source.Connect().FilterOnObservable(static _ => Observable.Create<bool>(observer =>
        {
            observer.OnNext(false);
            observer.OnNext(true);
            observer.OnNext(false);
            return Disposable.Empty;
        })).Transform(value => { transforms++; return value; }).RecordListItems(out var results);
        source.Add(1);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(transforms).IsEqualTo(0);
    }

    [Test]
    public async Task PendingPredicate_EntersOnlyAfterFirstTrueAndReleasesChildOnRemove()
    {
        using var source = new SourceList<int>();
        using var predicate = new ReactiveUI.Primitives.Signals.Signal<bool>();
        var transforms = 0;
        using var subscription = source.Connect().FilterOnObservable(_ => predicate)
            .Transform(value => { transforms++; return value; }).RecordListItems(out var results);
        source.Add(1);
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(transforms).IsEqualTo(0);
        predicate.OnNext(false);
        predicate.OnNext(true);
        predicate.OnNext(true);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 1 });
        await Assert.That(transforms).IsEqualTo(1);
        source.Remove(1);
        await Assert.That(predicate.HasObservers).IsFalse();
        predicate.OnNext(true);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(transforms).IsEqualTo(1);
    }

    [Test]
    public async Task CacheSameBatchAddRemove_SynchronousRefreshDoesNotLeaveStaleRow()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int, int>>();
        using var subscription = source.AutoRefreshOnObservable(static (_, _) => Observable.Return(Unit.Default))
            .ValidateChangeSets(static value => value).RecordCacheItems(out var results);
        source.OnNext(new ChangeSet<int, int>
        {
            new(ChangeReason.Add, 1, 1),
            new(ChangeReason.Remove, 1, 1)
        });

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(1);
        await Assert.That(results.RecordedChangeSets[0].Refreshes).IsEqualTo(0);
    }

    [Test]
    public async Task SourceCompletion_WaitsForLivePredicateAndThenReleasesIt()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var predicate = new ReactiveUI.Primitives.Signals.Signal<bool>();
        using var subscription = source.FilterOnObservable(_ => predicate).RecordListItems(out var results);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });
        source.OnCompleted();
        await Assert.That(results.HasCompleted).IsFalse();
        predicate.OnNext(true);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 1 });
        predicate.OnCompleted();

        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(predicate.HasObservers).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ErrorOrDisposal_ReleasesPredicateAndPreventsLaterChanges(bool error)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var predicate = new ReactiveUI.Primitives.Signals.Signal<bool>();
        using var subscription = source.FilterOnObservable(_ => predicate).RecordListItems(out var results);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });
        var failure = new InvalidOperationException("source");
        if (error)
            source.OnError(failure);
        else
            subscription.Dispose();
        var count = results.RecordedChangeSets.Count;
        predicate.OnNext(true);

        await Assert.That(predicate.HasObservers).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(count);
        await Assert.That(results.Error).IsSameReferenceAs(error ? failure : null);
        await Assert.That(results.HasCompleted).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InitialPredicateError_FailsWithoutTransformingExcludedRow(bool throwInFactory)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        var failure = new InvalidOperationException("predicate");
        var transforms = 0;
        using var subscription = source.FilterOnObservable(_ => throwInFactory ? throw failure : Observable.Throw<bool>(failure))
            .Transform(value => { transforms++; return value; }).RecordListItems(out var results);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });

        await Assert.That(results.Error).IsSameReferenceAs(failure);
        await Assert.That(transforms).IsEqualTo(0);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task BufferedPredicate_InitialFalseIsExcludedAndLaterTrueUsesWindow()
    {
        var scheduler = new TestScheduler();
        using var source = new SourceList<int>();
        using var predicate = new ReactiveUI.Primitives.Signals.Signal<bool>();
        var transforms = 0;
        using var subscription = source.Connect().FilterOnObservable(_ => predicate.StartWith(false), TimeSpan.FromSeconds(1), scheduler)
            .Transform(value => { transforms++; return value; }).RecordListItems(out var results);
        source.Add(1);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(transforms).IsEqualTo(0);
        predicate.OnNext(true);
        await Assert.That(results.RecordedItems).IsEmpty();
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 1 });
        await Assert.That(transforms).IsEqualTo(1);
    }

    [Test]
    public async Task LivePredicateError_ReleasesSiblingConnectionsAndSource()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var first = new ReactiveUI.Primitives.Signals.Signal<bool>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<bool>();
        var disposed = new int[2];
        using var subscription = source.FilterOnObservable(value => Observable.Create<bool>(observer =>
        {
            var child = (value == 1 ? first : second).Subscribe(observer);
            return Disposable.Create(() => { child.Dispose(); disposed[value - 1]++; });
        })).RecordListItems(out var results);
        source.OnNext(new ChangeSet<int>
        {
            new(ListChangeReason.Add, 1, 0),
            new(ListChangeReason.Add, 2, 1)
        });
        first.OnNext(true);
        second.OnNext(true);
        var failure = new InvalidOperationException("live predicate");
        first.OnError(failure);
        var count = results.RecordedChangeSets.Count;
        second.OnNext(false);

        await Assert.That(results.Error).IsSameReferenceAs(failure);
        await Assert.That(disposed).IsEquivalentTo(new[] { 1, 1 });
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(second.HasObservers).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(count);
    }
}
