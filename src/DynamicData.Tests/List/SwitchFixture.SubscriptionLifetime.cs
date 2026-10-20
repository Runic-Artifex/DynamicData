using System;
using System.Collections.Generic;
using System.Linq;
using DynamicData.Tests.Utilities;

namespace DynamicData.Tests.List;

// The list counterpart of the cache Switch subscription lifetime regressions: a source selected while an earlier
// Subscribe or Dispose frame is still on the stack keeps exclusive subscription ownership, superseded sources
// contribute nothing, and the latest selection wins.
public partial class SwitchFixture
{
    [Test]
    public async Task ReplacementReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        int? resourceOwner = null;
        var released = new List<int>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(CreateExclusiveSource(1));
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        switchable.OnNext(CreateExclusiveSource(2));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(resourceOwner).IsEqualTo(2);
        await Assert.That(released).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(new[] { 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(switchable.HasObservers).IsFalse();
        await Assert.That(results.HasCompleted).IsFalse();

        IObservable<IChangeSet<int>> CreateExclusiveSource(int item) =>
            CreateExclusiveListSource(item, () => resourceOwner, owner => resourceOwner = owner, released);
    }

    [Test]
    public async Task ReentrantSelectionDuringSynchronousAddReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        int? resourceOwner = null;
        var released = new List<int>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                // Selects the replacement before the first Subscribe has returned its disposable.
                if (changes.Any(change => change.Reason is ListChangeReason.Add && change.Item.Current == 1))
                    switchable.OnNext(CreateExclusiveSource(2));
            })
            .RecordListItems(out var results);

        switchable.OnNext(CreateExclusiveSource(1));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(resourceOwner).IsEqualTo(2);
        await Assert.That(released).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(DescribeChanges(results)).IsEquivalentTo(new[] { "Add 1", "Clear [1]", "Add 2" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(new[] { 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        IObservable<IChangeSet<int>> CreateExclusiveSource(int item) =>
            CreateExclusiveListSource(item, () => resourceOwner, owner => resourceOwner = owner, released);
    }

    [Test]
    public async Task ReentrantSelectionDuringTeardownReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        int? resourceOwner = null;
        var released = new List<int>();
        var supersededSubscriptions = 0;
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        var superseded = Observable.Defer(() =>
        {
            supersededSubscriptions++;
            return Observable.Never<IChangeSet<int>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        // The first source's teardown selects the latest while it still owns the resource.
        switchable.OnNext(CreateExclusiveListSource(1, () => resourceOwner, owner => resourceOwner = owner, released, teardown: () => switchable.OnNext(CreateExclusiveSource(3))));
        switchable.OnNext(superseded);

        await Assert.That(supersededSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(resourceOwner).IsEqualTo(3);
        await Assert.That(released).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(DescribeChanges(results)).IsEquivalentTo(new[] { "Add 1", "Clear [1]", "Add 3" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(new[] { 1, 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        IObservable<IChangeSet<int>> CreateExclusiveSource(int item) =>
            CreateExclusiveListSource(item, () => resourceOwner, owner => resourceOwner = owner, released);
    }

    [Test]
    [Arguments(null)]
    [Arguments(NotificationKind.OnCompleted)]
    [Arguments(NotificationKind.OnError)]
    public async Task ReentrantSelectionDuringSynchronousAddKeepsLatestSubscription(NotificationKind? supersededTermination)
    {
        var supersededDisposals = 0;
        var supersededError = new InvalidOperationException("The superseded source failed before Subscribe returned.");
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var latest = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        var first = Observable.Create<IChangeSet<int>>(observer =>
        {
            // The downstream callback selects latest before this Subscribe can return its disposable.
            observer.OnNext(AddItem(1));
            if (supersededTermination is NotificationKind.OnCompleted)
                observer.OnCompleted();
            else if (supersededTermination is NotificationKind.OnError)
                observer.OnError(supersededError);

            return Disposable.Create(() => supersededDisposals++);
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ListChangeReason.Add && change.Item.Current == 1))
                    switchable.OnNext(latest);
            })
            .RecordListItems(out var results);

        switchable.OnNext(first);
        var latestRemainedSubscribed = latest.HasObservers;
        latest.OnNext(AddItem(2));
        switchable.OnCompleted();
        var completedBeforeLatest = results.HasCompleted;
        latest.OnCompleted();

        await Assert.That(results.Error).IsNull();
        await Assert.That(latestRemainedSubscribed).IsTrue();
        await Assert.That(supersededDisposals).IsEqualTo(1);
        await Assert.That(completedBeforeLatest).IsFalse();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(DescribeChanges(results)).IsEquivalentTo(new[] { "Add 1", "Clear [1]", "Add 2" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringResetReleasesSupersededReplacementBeforeSubscribingLatest()
    {
        // The clear waits for the replacement's snapshot, so the replacement is subscribed before it is
        // published. A selection made while subscribers handle the clear must still release it before the
        // latest source is subscribed, and it must contribute nothing.
        var events = new List<string>();
        using var first = new SourceList<int>();
        using var latest = new TestSourceList<int>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        first.Add(1);
        latest.Add(2);
        var superseded = Observable.Create<IChangeSet<int>>(_ =>
        {
            events.Add("superseded subscribed");
            return Disposable.Create(() => events.Add("superseded released"));
        });
        var latestConnection = Observable.Defer(() =>
        {
            events.Add("latest subscribed");
            return latest.Connect();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ListChangeReason.Clear))
                    switchable.OnNext(latestConnection);
            })
            .RecordListItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(superseded);
        latest.Add(3);
        switchable.OnCompleted();
        latest.Complete();

        await Assert.That(events).IsEquivalentTo(new[] { "superseded subscribed", "superseded released", "latest subscribed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2, 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(DescribeChanges(results)).IsEquivalentTo(new[] { "AddRange [1]", "Clear [1]", "AddRange [2]", "Add 3" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringDisposalDoesNotSubscribeSupersededReplacement()
    {
        var firstDisposals = 0;
        var supersededSubscriptions = 0;
        using var latest = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        var first = Observable.Create<IChangeSet<int>>(observer =>
        {
            observer.OnNext(AddItem(1));
            return Disposable.Create(() =>
            {
                firstDisposals++;
                switchable.OnNext(latest);
            });
        });
        var superseded = Observable.Defer(() =>
        {
            supersededSubscriptions++;
            return Observable.Never<IChangeSet<int>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(superseded);
        latest.OnNext(AddItem(2));
        switchable.OnCompleted();
        latest.OnCompleted();

        await Assert.That(firstDisposals).IsEqualTo(1);
        await Assert.That(supersededSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(DescribeChanges(results)).IsEquivalentTo(new[] { "Add 1", "Clear [1]", "Add 2" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DisposalDuringResetReleasesReplacement()
    {
        // The clear waits for the replacement's snapshot, so the replacement is already subscribed when
        // subscribers see it; disposing then must release it and stop everything it would deliver.
        var replacementSubscriptions = 0;
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(1));
        using var replacementChanges = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        var replacement = Observable.Defer(() =>
        {
            replacementSubscriptions++;
            return replacementChanges;
        });
        var results = new ListItemRecordingObserver<int>(Scheduler.Immediate);
        IObserver<IChangeSet<int>> recorder = results;
        using var subscription = new ReactiveUI.Primitives.Disposables.OnceDisposable();
        subscription.Disposable = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Subscribe(
                changes =>
                {
                    // Record the clear before disposing, so the expected observable state is unambiguous.
                    recorder.OnNext(changes);
                    if (changes.Any(change => change.Reason is ListChangeReason.Clear))
                        subscription.Dispose();
                },
                recorder.OnError,
                recorder.OnCompleted);

        switchable.OnNext(first);
        switchable.OnNext(replacement);
        var notificationsAtDisposal = results.Notifications.ToArray();
        replacementChanges.OnNext(AddItem(2));
        replacementChanges.OnCompleted();

        await Assert.That(replacementSubscriptions).IsEqualTo(1);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(replacementChanges.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(results.Notifications.SequenceEqual(notificationsAtDisposal)).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsFalse();
    }

    [Test]
    public async Task OuterErrorDuringResetReleasesReplacement()
    {
        // The clear waits for the replacement's snapshot, so the replacement is already subscribed when
        // the outer error arrives during the clear; the error must release it.
        var error = new InvalidOperationException("The outer source failed during reset.");
        var replacementSubscriptions = 0;
        var replacementReleases = 0;
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(1));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        var replacement = Observable.Create<IChangeSet<int>>(_ =>
        {
            replacementSubscriptions++;
            return Disposable.Create(() => replacementReleases++);
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ListChangeReason.Clear))
                    switchable.OnError(error);
            })
            .RecordListItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(replacement);

        await Assert.That(replacementSubscriptions).IsEqualTo(1);
        await Assert.That(replacementReleases).IsEqualTo(1);
        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.RecordedItems).IsEmpty();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
    }

    [Test]
    public async Task OuterCompletionDuringResetWaitsForReplacement()
    {
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(1));
        using var replacement = new TestSourceList<int>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ListChangeReason.Clear))
                    switchable.OnCompleted();
            })
            .RecordListItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(replacement.Connect());
        await Assert.That(results.HasCompleted).IsFalse();

        replacement.Add(2);
        replacement.Complete();

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(first.HasObservers).IsFalse();
    }

    [Test]
    public async Task SynchronousOuterSubscriptionFailureReleasesActivatedInner()
    {
        var error = new InvalidOperationException("The outer Subscribe failed after activating an inner source.");
        var innerDisposals = 0;
        using var inner = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(1));
        var source = RawAnonymousObservable.Create<IObservable<IChangeSet<int>>>(observer =>
        {
            observer.OnNext(inner.Finally(() => innerDisposals++));
            throw error;
        });
        using var subscription = source.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(inner.HasObservers).IsFalse();
        await Assert.That(innerDisposals).IsEqualTo(1);
    }

    [Test]
    public async Task SubscriptionsKeepIndependentCurrentSources()
    {
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(1));
        using var second = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<int>>(AddItem(2));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        var switched = switchable.Switch();
        using var firstSubscription = switched
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var firstResults);
        using var secondSubscription = switched
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var secondResults);

        switchable.OnNext(first);
        firstSubscription.Dispose();
        switchable.OnNext(second);
        switchable.OnCompleted();
        second.OnCompleted();

        await Assert.That(firstResults.Error).IsNull();
        await Assert.That(firstResults.HasCompleted).IsFalse();
        await Assert.That(firstResults.RecordedItems).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(secondResults.Error).IsNull();
        await Assert.That(secondResults.HasCompleted).IsTrue();
        await Assert.That(secondResults.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(second.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
    }

    [Test]
    public async Task IgnoresChangesFromASupersededSource()
    {
        var supersededObserver = default(IObserver<IChangeSet<int>>);
        var superseded = RawAnonymousObservable.Create<IChangeSet<int>>(observer =>
        {
            supersededObserver = observer;
            observer.OnNext(AddItem(1));
            return Disposable.Empty;
        });
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var current = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(superseded);
        switchable.OnNext(current);

        // Disposal cannot reach a notification already in flight, so the raw observer stands in for one.
        await Assert.That(supersededObserver).IsNotNull();
        supersededObserver!.OnNext(AddItem(9));
        supersededObserver.OnCompleted();
        switchable.OnCompleted();

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.RecordedItems).IsEmpty();

        current.OnNext(AddItem(2));
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task IgnoresErrorsFromASupersededSource()
    {
        var supersededObserver = default(IObserver<IChangeSet<int>>);
        var superseded = RawAnonymousObservable.Create<IChangeSet<int>>(observer =>
        {
            supersededObserver = observer;
            return Disposable.Empty;
        });
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<int>>>();
        using var current = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets()
            .RecordListItems(out var results);

        switchable.OnNext(superseded);
        switchable.OnNext(current);

        await Assert.That(supersededObserver).IsNotNull();
        supersededObserver!.OnError(new InvalidOperationException("The superseded source failed after the switch."));

        await Assert.That(results.Error).IsNull();

        current.OnNext(AddItem(2));
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    private static IObservable<IChangeSet<int>> CreateExclusiveListSource(int item, Func<int?> getOwner, Action<int?> setOwner, List<int> released, Action? teardown = null) =>
        Observable.Create<IChangeSet<int>>(observer =>
        {
            if (getOwner() is not null)
            {
                observer.OnError(new InvalidOperationException("The previous subscription still owns the resource."));
                return Disposable.Empty;
            }

            setOwner(item);
            observer.OnNext(AddItem(item));
            return Disposable.Create(() =>
            {
                teardown?.Invoke();
                released.Add(item);
                setOwner(null);
            });
        });

    private static IChangeSet<int> AddItem(int item) =>
        new ChangeSet<int> { new(ListChangeReason.Add, item, 0) };

    private static string[] DescribeChanges(ListItemRecordingObserver<int> results) =>
        results.RecordedChangeSets
            .SelectMany(changes => changes)
            .Select(change => change.Type is ChangeType.Item
                ? $"{change.Reason} {change.Item.Current}"
                : $"{change.Reason} [{string.Join(",", change.Range)}]")
            .ToArray();
}
