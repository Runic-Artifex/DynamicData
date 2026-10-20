#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.List;

// Switching between populated sources publishes the clear and the new source's snapshot as one changeset,
// so subscribers never observe the empty list in between.
public class SwitchHandoffFixture
{
    private static readonly Exception HandoffError = new InvalidOperationException("The new source failed during the handoff.");

    [Test]
    public async Task HandsOffPopulatedSourcesInOneChangeset()
    {
        using var first = new SourceList<int>();
        using var second = new SourceList<int>();
        first.AddRange(new[] { 1, 2, 3 });
        second.AddRange(new[] { 4, 5 });
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(first.Connect());
        var cloned = new List<int>();
        var bound = new ObservableCollectionExtended<int>();
        var counts = new List<int>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Clone(cloned)
            .Bind(bound)
            .Do(_ => counts.Add(bound.Count))
            .RecordListItems(out var results);

        switchable.OnNext(second.Connect());

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        var handoff = results.RecordedChangeSets[1];
        await Assert.That(handoff.Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear, ListChangeReason.AddRange }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(handoff.First().Range).IsEquivalentTo(new[] { 1, 2, 3 });
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 4, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(cloned).IsEquivalentTo(new[] { 4, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(bound).IsEquivalentTo(new[] { 4, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(counts).DoesNotContain(0);

        second.Insert(0, 6);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(3);
        await Assert.That(bound).IsEquivalentTo(new[] { 6, 4, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task FoldsOnlyTheFirstSynchronousChangeset()
    {
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(Observable.Return(AddItems(0, 1, 2)).Concat(Observable.Never<IChangeSet<int>>()));
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(Observable.Create<IChangeSet<int>>(observer =>
        {
            observer.OnNext(AddItems(0, 3));
            observer.OnNext(AddItems(0, 4));
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Skip(1).Select(changes => changes.Select(change => change.Reason).ToArray()).ToArray())
            .IsEquivalentTo(new[] { new[] { ListChangeReason.Clear, ListChangeReason.AddRange }, new[] { ListChangeReason.AddRange } }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 4, 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task HandoffWaitsPastAnInitialEmptyChangeset()
    {
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItems(0, 1)));
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(Observable.Create<IChangeSet<int>>(observer =>
        {
            observer.OnNext(ChangeSet<int>.Empty);
            observer.OnNext(AddItems(0, 2));
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear, ListChangeReason.AddRange }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 });
    }

    [Test]
    public async Task DelayedSourcePublishesClearFirst()
    {
        using var first = new SourceList<int>();
        first.AddRange(new[] { 1, 2 });
        using var delayed = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(delayed);

        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear });
        await Assert.That(results.RecordedItems).IsEmpty();

        delayed.OnNext(AddItems(0, 3));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(3);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 3 });
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task EmptySourcePublishesOnlyTheClear(bool emitsInitialEmptyChangeset)
    {
        using var first = new SourceList<int>();
        first.AddRange(new[] { 1, 2 });
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(emitsInitialEmptyChangeset
            ? Observable.Return(ChangeSet<int>.Empty).Concat(Observable.Never<IChangeSet<int>>())
            : Observable.Never<IChangeSet<int>>());

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear });
        await Assert.That(results.RecordedItems).IsEmpty();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ErrorDuringHandoffFollowsWhatWasOwed(bool emitsSnapshotFirst)
    {
        using var first = new SourceList<int>();
        first.AddRange(new[] { 1, 2 });
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(emitsSnapshotFirst
            ? Observable.Return(AddItems(0, 3)).Concat(Observable.Throw<IChangeSet<int>>(HandoffError))
            : Observable.Throw<IChangeSet<int>>(HandoffError));

        await Assert.That(results.Error).IsSameReferenceAs(HandoffError);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => change.Reason))
            .IsEquivalentTo(emitsSnapshotFirst ? new[] { ListChangeReason.Clear, ListChangeReason.AddRange } : new[] { ListChangeReason.Clear }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OuterErrorDuringHandoffPublishesClearFirst()
    {
        var error = new InvalidOperationException("The outer source failed during the handoff.");
        using var first = new SourceList<int>();
        first.AddRange(new[] { 1, 2 });
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(Observable.Create<IChangeSet<int>>(_ =>
        {
            switchable.OnError(error);
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear });
    }

    [Test]
    public async Task CompletionDuringHandoffPublishesClearFirst()
    {
        using var first = new SourceList<int>();
        first.AddRange(new[] { 1, 2 });
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(Observable.Create<IChangeSet<int>>(observer =>
        {
            // Both sequences end before the new source has delivered anything.
            switchable.OnCompleted();
            observer.OnCompleted();
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringHandoffKeepsLatestSource()
    {
        var events = new List<string>();
        using var first = new SourceList<int>();
        using var second = new SourceList<int>();
        using var latest = new SourceList<int>();
        first.AddRange(new[] { 1 });
        second.AddRange(new[] { 2 });
        latest.AddRange(new[] { 3 });
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<int>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                // Selects the latest source while the handoff batch from the second source is being delivered.
                if (changes.Any(change => change.Reason is ListChangeReason.AddRange && change.Range.Contains(2)))
                    switchable.OnNext(Track("latest", latest.Connect()));
            })
            .RecordListItems(out var results);

        switchable.OnNext(Track("second", second.Connect()));

        await Assert.That(results.Error).IsNull();
        await Assert.That(events).IsEquivalentTo(new[] { "second subscribed", "second released", "latest subscribed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(3);
        await Assert.That(results.RecordedChangeSets[2].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Clear, ListChangeReason.AddRange }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 3 });

        IObservable<T> Track<T>(string name, IObservable<T> source) => Observable.Create<T>(observer =>
        {
            events.Add($"{name} subscribed");
            var inner = source.Subscribe(observer);
            return Disposable.Create(() =>
            {
                inner.Dispose();
                events.Add($"{name} released");
            });
        });
    }

    private static IChangeSet<int> AddItems(int index, params int[] items) =>
        new ChangeSet<int> { new(ListChangeReason.AddRange, items, index) };
}
