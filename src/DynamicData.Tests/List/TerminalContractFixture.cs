#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.List;

public class TerminalContractFixture
{
    [Test]
    public async Task QueryForwardsTerminalsAndHasIndependentSubscriptionState()
    {
        using var source = new Signal<IChangeSet<int>>();
        var query = source.QueryWhenChanged();
        var first = new List<int>();
        var second = new List<int>();
        Exception? error = null;
        using var subscription1 = query.Subscribe(values => first = values.ToList(), ex => error = ex);
        source.OnNext(Add(1));
        using var subscription2 = query.Subscribe(values => second = values.ToList(), _ => { });
        source.OnNext(Add(2));
        await Assert.That(first).IsEquivalentTo(new[] { 1, 2 });
        await Assert.That(second).IsEquivalentTo(new[] { 2 });
        var expected = new InvalidOperationException("query");
        source.OnError(expected);
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(source.HasObservers).IsFalse();
        var completed = false;
        using var empty = Observable.Empty<IChangeSet<int>>().QueryWhenChanged().Subscribe(_ => { }, () => completed = true);
        await Assert.That(completed).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BufferFlushesBeforeCompletionAndDisposesControls(bool synchronous)
    {
        using var source = new Signal<IChangeSet<int>>();
        using var pause = new Signal<bool>();
        var events = new List<string>();
        IObservable<IChangeSet<int>> input = synchronous ? Observable.Return<IChangeSet<int>>(Add(1)) : source;
        using var subscription = input.BufferIf(pause, initialPauseState: true, scheduler: Scheduler.Immediate)
            .Subscribe(changes => events.Add($"next:{changes.Adds}"), _ => events.Add("error"), () => events.Add("complete"));
        if (!synchronous)
        {
            source.OnNext(Add(1));
            source.OnCompleted();
        }

        await Assert.That(events).IsEquivalentTo(new[] { "next:1", "complete" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(pause.HasObservers).IsFalse();
        pause.OnNext(false);
        await Assert.That(events.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BufferDiscardsOnEitherErrorAndReleasesSubscriptions(bool controlError)
    {
        using var source = new Signal<IChangeSet<int>>();
        using var pause = new Signal<bool>();
        var values = 0;
        var completions = 0;
        var errors = new List<Exception>();
        using var subscription = source.BufferIf(pause, initialPauseState: true, scheduler: Scheduler.Immediate)
            .Subscribe(_ => values++, errors.Add, () => completions++);
        source.OnNext(Add(1));
        var expected = new InvalidOperationException("buffer");
        if (controlError)
        {
            pause.OnError(expected);
        }
        else
        {
            source.OnError(expected);
        }

        pause.OnNext(false);
        source.OnNext(Add(2));
        await Assert.That(errors).IsEquivalentTo(new[] { expected });
        await Assert.That(values).IsEqualTo(0);
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(pause.HasObservers).IsFalse();
    }

    [Test]
    public async Task BufferDetachesFlushedBatchBeforeReentrantSourceUpdate()
    {
        using var source = new Signal<IChangeSet<int>>();
        using var pause = new Signal<bool>();
        var batches = new List<IChangeSet<int>>();
        using var subscription = source.BufferIf(pause, initialPauseState: true, scheduler: Scheduler.Immediate).Subscribe(changes =>
        {
            batches.Add(changes);
            if (batches.Count == 1)
            {
                pause.OnNext(true);
                source.OnNext(Add(2));
            }
        });
        source.OnNext(Add(1));
        pause.OnNext(false);
        source.OnCompleted();
        await Assert.That(batches.SelectMany(changes => changes).Select(change => change.Item.Current)).IsEquivalentTo(new[] { 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(batches.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task StaticCombinerWaitsForAllSourcesAndFailsFast(int operation)
    {
        using var first = new Signal<IChangeSet<int>>();
        using var second = new Signal<IChangeSet<int>>();
        var completed = 0;
        using var subscription = Combine(new IObservable<IChangeSet<int>>[] { first, second }, operation).Subscribe(_ => { }, _ => { }, () => completed++);
        first.OnCompleted();
        await Assert.That(completed).IsEqualTo(0);
        second.OnCompleted();
        await Assert.That(completed).IsEqualTo(1);
        var emptyCompleted = false;
        using var empty = Combine(Array.Empty<IObservable<IChangeSet<int>>>(), operation).Subscribe(_ => { }, () => emptyCompleted = true);
        await Assert.That(emptyCompleted).IsTrue();

        using var failing = new Signal<IChangeSet<int>>();
        using var live = new Signal<IChangeSet<int>>();
        Exception? received = null;
        using var failure = Combine(new IObservable<IChangeSet<int>>[] { failing, live }, operation).Subscribe(_ => { }, error => received = error);
        var expected = new InvalidOperationException("combiner");
        failing.OnError(expected);
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(live.HasObservers).IsFalse();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task DynamicCombinerWaitsForLiveChildAfterParentCompletes(int operation)
    {
        using var parent = new SourceList<IObservable<IChangeSet<int>>>();
        using var child = new Signal<IChangeSet<int>>();
        parent.Add(child);
        var completed = false;
        var updates = 0;
        using var subscription = Combine(parent, operation).Subscribe(_ => updates++, _ => { }, () => completed = true);
        parent.Dispose();
        await Assert.That(completed).IsFalse();
        child.OnNext(Add(1));
        await Assert.That(updates).IsEqualTo(1);
        child.OnCompleted();
        await Assert.That(completed).IsTrue();
        await Assert.That(child.HasObservers).IsFalse();
    }

    [Test]
    public async Task DynamicCombinerChildErrorAfterParentCompletionFailsWithoutCompletion()
    {
        using var parent = new SourceList<IObservable<IChangeSet<int>>>();
        using var child = new Signal<IChangeSet<int>>();
        parent.Add(child);
        Exception? received = null;
        var completed = false;
        using var subscription = parent.Or().Subscribe(_ => { }, error => received = error, () => completed = true);
        parent.Dispose();
        var expected = new InvalidOperationException("child");
        child.OnError(expected);
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    public async Task DynamicCombinerRemovedChildDoesNotDelayCompletion()
    {
        using var parent = new SourceList<IObservable<IChangeSet<int>>>();
        using var child = new Signal<IChangeSet<int>>();
        parent.Add(child);
        var completed = false;
        using var subscription = parent.Or().Subscribe(_ => { }, () => completed = true);
        parent.Remove(child);
        await Assert.That(child.HasObservers).IsFalse();
        parent.Dispose();
        await Assert.That(completed).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GroupingWithoutRegrouperForwardsCompletionAndError(bool immutable)
    {
        using var source = new Signal<IChangeSet<int>>();
        var completed = false;
        using var subscription = immutable
            ? source.GroupWithImmutableState(value => value % 2).Subscribe(_ => { }, () => completed = true)
            : source.GroupOn(value => value % 2).Subscribe(_ => { }, () => completed = true);
        source.OnNext(Add(1));
        source.OnCompleted();
        await Assert.That(completed).IsTrue();
        var expected = new InvalidOperationException("group");
        Exception? received = null;
        using var errorSubscription = immutable
            ? Observable.Throw<IChangeSet<int>>(expected).GroupWithImmutableState(value => value).Subscribe(_ => { }, error => received = error)
            : Observable.Throw<IChangeSet<int>>(expected).GroupOn(value => value).Subscribe(_ => { }, error => received = error);
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task SortCompletesWithoutControlsAndWaitsForSuppliedControls()
    {
        var completed = false;
        using var sync = Observable.Return(Add(1)).Sort(Comparer<int>.Default).Subscribe(_ => { }, () => completed = true);
        await Assert.That(completed).IsTrue();
        using var source = new Signal<IChangeSet<int>>();
        using var resort = new Signal<Unit>();
        completed = false;
        Exception? received = null;
        using var subscription = source.Sort(Comparer<int>.Default, resort: resort).Subscribe(_ => { }, error => received = error, () => completed = true);
        source.OnCompleted();
        await Assert.That(completed).IsFalse();
        var expected = new InvalidOperationException("sort control");
        resort.OnError(expected);
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    public async Task ComparerMergeChangeSetsOverloadsForwardAndWaitForActiveChildren()
    {
        using var parent = new SourceList<IObservable<IChangeSet<int, int>>>();
        using var child = new Signal<IChangeSet<int, int>>();
        parent.Add(child);
        var completed = false;
        var updates = 0;
        using var subscription = parent.Connect().MergeChangeSets(Comparer<int>.Default).Subscribe(_ => updates++, () => completed = true);
        parent.Dispose();
        await Assert.That(completed).IsFalse();
        child.OnNext(new ChangeSet<int, int> { new(ChangeReason.Add, 1, 1) });
        await Assert.That(updates).IsEqualTo(1);
        child.OnCompleted();
        await Assert.That(completed).IsTrue();
        using var emptyParent = new SourceList<IObservable<IChangeSet<int, int>>>();
        completed = false;
        using var listOverload = emptyParent.MergeChangeSets(Comparer<int>.Default).Subscribe(_ => { }, () => completed = true);
        emptyParent.Dispose();
        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task MergeManySynchronousReplacementPreservesDuplicateDestinationOccurrences()
    {
        using var parent = new SourceList<IObservable<IChangeSet<int>>>();
        parent.Add(Observable.Return(Add(1)));
        using var results = parent.Connect().MergeManyChangeSets(child => child).AsAggregator();
        parent.ReplaceAt(0, Observable.Return(new ChangeSet<int> { new(ListChangeReason.AddRange, new[] { 1, 1 }, 0) }));
        await Assert.That(results.Exception).IsNull();
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1 });
        parent.Clear();
        await Assert.That(results.Data.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments("query-selector")]
    [Arguments("collection")]
    [Arguments("sorted-selector")]
    [Arguments("sorted-comparer")]
    [Arguments("maximum")]
    [Arguments("minimum")]
    public async Task QueryDerivedOperatorsForwardTerminals(string operation)
    {
        using var source = new Signal<IChangeSet<int>>();
        var completed = false;
        using var subscription = QueryPipeline(source, operation).Subscribe(_ => { }, () => completed = true);
        source.OnNext(Add(1));
        source.OnCompleted();
        await Assert.That(completed).IsTrue();
        var expected = new InvalidOperationException("query-derived");
        Exception? received = null;
        using var failure = QueryPipeline(Observable.Throw<IChangeSet<int>>(expected), operation).Subscribe(_ => { }, error => received = error);
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SuppliedRegrouperKeepsOutputActiveAndForwardsItsError(bool immutable)
    {
        using var source = new Signal<IChangeSet<int>>();
        using var regrouper = new Signal<Unit>();
        var completed = false;
        Exception? received = null;
        using var subscription = immutable
            ? source.GroupWithImmutableState(value => value, regrouper).Subscribe(_ => { }, error => received = error, () => completed = true)
            : source.GroupOn(value => value, regrouper).Subscribe(_ => { }, error => received = error, () => completed = true);
        source.OnNext(Add(1));
        regrouper.OnNext(Unit.Default);
        source.OnCompleted();
        await Assert.That(completed).IsFalse();
        var expected = new InvalidOperationException("regrouper");
        regrouper.OnError(expected);
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
        await Assert.That(regrouper.HasObservers).IsFalse();
    }

    [Test]
    public async Task DisposeManyTracksOwnershipBeforeReentrantDisposal()
    {
        using var source = new Signal<IChangeSet<TrackedDisposable>>();
        var item = new TrackedDisposable();
        IDisposable? subscription = null;
        subscription = source.DisposeMany().Subscribe(_ => subscription!.Dispose());
        source.OnNext(new ChangeSet<TrackedDisposable> { new(ListChangeReason.Add, item, 0) });
        await Assert.That(item.Disposals).IsEqualTo(1);
        await Assert.That(source.HasObservers).IsFalse();
        subscription.Dispose();
        await Assert.That(item.Disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MergeManyFactoryExceptionsFailInsteadOfHanging(bool disposedException)
    {
        Exception expected = disposedException ? new ObjectDisposedException("factory") : new InvalidOperationException("factory");
        Exception? received = null;
        var completed = false;
        using var subscription = Observable.Return<IChangeSet<int>>(Add(1))
            .MergeMany<int, int>(_ => throw expected)
            .Subscribe(_ => { }, error => received = error, () => completed = true);
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    [Arguments(ListChangeReason.Remove)]
    [Arguments(ListChangeReason.RemoveRange)]
    [Arguments(ListChangeReason.Clear)]
    [Arguments(ListChangeReason.Replace)]
    public async Task DisposeManyReleasesRemovedOwnershipWhenObserverThrows(ListChangeReason reason)
    {
        using var source = new SourceList<TrackedDisposable>();
        var removed = new TrackedDisposable();
        var replacement = new TrackedDisposable();
        var expected = new InvalidOperationException("observer");
        var observerThrew = false;
        using var subscription = source.Connect().DisposeMany().Subscribe(changes =>
        {
            if (changes.Any(change => change.Reason == reason))
            {
                observerThrew = true;
                throw expected;
            }
        }, _ => { });
        source.Add(removed);
        try
        {
            switch (reason)
            {
                case ListChangeReason.Remove:
                    source.RemoveAt(0);
                    break;
                case ListChangeReason.RemoveRange:
                    source.RemoveRange(0, 1);
                    break;
                case ListChangeReason.Clear:
                    source.Clear();
                    break;
                default:
                    source.ReplaceAt(0, replacement);
                    break;
            }
        }
        catch (InvalidOperationException error) when (ReferenceEquals(error, expected))
        {
            // Observer delegate exception routing differs between the Rx flavors.
        }

        await Assert.That(observerThrew).IsTrue();
        await Assert.That(removed.Disposals).IsEqualTo(1);
        subscription.Dispose();
        await Assert.That(removed.Disposals).IsEqualTo(1);
        if (reason == ListChangeReason.Replace)
        {
            await Assert.That(replacement.Disposals).IsEqualTo(1);
        }
    }

    [Test]
    public async Task DisposeManyDetachesItemsBeforeReentrantItemDisposal()
    {
        using var source = new SourceList<TrackedDisposable>();
        IDisposable? subscription = null;
        var item = new TrackedDisposable { OnDispose = () => subscription!.Dispose() };
        subscription = source.Connect().DisposeMany().Subscribe(_ => { });
        source.Add(item);
        source.Dispose();
        await Assert.That(item.Disposals).IsEqualTo(1);
        subscription.Dispose();
        await Assert.That(item.Disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(0, false)]
    [Arguments(1, false)]
    [Arguments(2, false)]
    [Arguments(3, false)]
    [Arguments(0, true)]
    [Arguments(1, true)]
    [Arguments(2, true)]
    [Arguments(3, true)]
    public async Task CombinersEmitPopulatedSynchronousSnapshotsBeforeCompleting(int operation, bool dynamic)
    {
        IObservable<IChangeSet<int>>[] children =
        [
            Observable.Return<IChangeSet<int>>(Range(1, 2)),
            Observable.Return<IChangeSet<int>>(Range(2, 3)),
        ];
        using var parent = new SourceList<IObservable<IChangeSet<int>>>();
        parent.AddRange(children);
        parent.Dispose();
        using var results = (dynamic ? Combine(parent, operation) : Combine(children, operation)).AsAggregator();
        var expected = operation switch
        {
            0 => new[] { 2 },
            1 => new[] { 1, 2, 3 },
            2 => new[] { 1, 3 },
            _ => new[] { 1 },
        };
        await Assert.That(results.Data.Items).IsEquivalentTo(expected);
        await Assert.That(results.Exception).IsNull();
        await Assert.That(results.IsCompleted).IsTrue();
    }

    [Test]
    public async Task FiniteSortingAndGroupingEmitValuesBeforeCompletion()
    {
        var source = Observable.Return<IChangeSet<int>>(Range(3, 1, 2));
        using var sorted = source.Sort(Comparer<int>.Default).AsAggregator();
        await Assert.That(sorted.Data.Items).IsEquivalentTo(new[] { 1, 2, 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(sorted.IsCompleted).IsTrue();
        var grouped = Array.Empty<int>();
        var immutable = Array.Empty<int>();
        var completed = 0;
        using var groups = source.GroupOn(value => value % 2).ToCollection().Subscribe(values =>
            grouped = values.OrderBy(group => group.GroupKey).SelectMany(group => group.List.Items).ToArray(), () => completed++);
        using var immutableGroups = source.GroupWithImmutableState(value => value % 2).ToCollection().Subscribe(values =>
            immutable = values.OrderBy(group => group.Key).SelectMany(group => group.Items).ToArray(), () => completed++);
        await Assert.That(grouped).IsEquivalentTo(new[] { 3, 1, 2 });
        await Assert.That(immutable).IsEquivalentTo(new[] { 3, 1, 2 });
        await Assert.That(completed).IsEqualTo(2);
    }

    private static ChangeSet<int> Range(params int[] values) => new() { new(ListChangeReason.AddRange, values, 0) };

    [Test]
    public async Task BufferDisposalPreventsCapturedTimeoutWorkFromNotifying()
    {
        var scheduler = new FakeScheduler();
        using var source = new Signal<IChangeSet<int>>();
        using var pause = new Signal<bool>();
        var notifications = 0;
        using var subscription = source.BufferIf(pause, true, TimeSpan.FromMilliseconds(10), scheduler)
            .Subscribe(_ => notifications++, _ => notifications++, () => notifications++);
        foreach (var action in scheduler.ScheduledActions.ToArray())
        {
            action.Invoke();
            scheduler.ScheduledActions.Remove(action);
        }

        source.OnNext(Add(1));
        var callbacks = scheduler.ScheduledActions.ToArray();
        await Assert.That(callbacks.Length).IsGreaterThan(0);
        subscription.Dispose();
        scheduler.Now += TimeSpan.FromMilliseconds(20);
        // Invoke captured scheduler work after cancellation; the subscription must suppress delivery.
        foreach (var callback in callbacks)
        {
            callback.Invoke();
        }

        await Assert.That(notifications).IsEqualTo(0);
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(pause.HasObservers).IsFalse();
    }

    [Test]
    public async Task PauseControlCompletionDoesNotTerminateTheBufferedSource()
    {
        using var source = new Signal<IChangeSet<int>>();
        var completed = false;
        var updates = 0;
        using var subscription = source.BufferIf(Observable.Empty<bool>(), scheduler: Scheduler.Immediate)
            .Subscribe(_ => updates++, () => completed = true);
        await Assert.That(completed).IsFalse();
        source.OnNext(Add(1));
        source.OnCompleted();
        await Assert.That(updates).IsEqualTo(1);
        await Assert.That(completed).IsTrue();
    }

    private static IObservable<int> QueryPipeline(IObservable<IChangeSet<int>> source, string operation) => operation switch
    {
        "query-selector" => source.QueryWhenChanged(values => values.Count),
        "collection" => source.ToCollection().Select(values => values.Count),
        "sorted-selector" => source.ToSortedCollection(value => value).Select(values => values.Count),
        "sorted-comparer" => source.ToSortedCollection(Comparer<int>.Default).Select(values => values.Count),
        "maximum" => source.Maximum(value => value),
        _ => source.Minimum(value => value),
    };

    private sealed class TrackedDisposable : IDisposable
    {
        public int Disposals { get; private set; }

        public Action? OnDispose { get; init; }

        public void Dispose()
        {
            Disposals++;
            OnDispose?.Invoke();
        }
    }

    private static ChangeSet<int> Add(int value) => new() { new(ListChangeReason.Add, value, 0) };

    private static IObservable<IChangeSet<int>> Combine(ICollection<IObservable<IChangeSet<int>>> sources, int operation) => operation switch
    {
        0 => sources.And(),
        1 => sources.Or(),
        2 => sources.Xor(),
        _ => sources.Except(),
    };

    private static IObservable<IChangeSet<int>> Combine(IObservableList<IObservable<IChangeSet<int>>> sources, int operation) => operation switch
    {
        0 => sources.And(),
        1 => sources.Or(),
        2 => sources.Xor(),
        _ => sources.Except(),
    };
}
