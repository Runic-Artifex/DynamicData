using System;
using System.Collections.Generic;
using System.Linq;
using DynamicData.Tests.Domain;
using DynamicData.Tests.Utilities;

namespace DynamicData.Tests.Cache;

public partial class SwitchFixture
{


    [Test]
    public async Task ReplacementReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        var people = CreateSubscriptionPeople(2);
        Person? resourceOwner = null;
        var released = new List<Person>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(CreateExclusiveSource(people[0]));
        await Assert.That(results.RecordedItemsByKey.Values).IsEquivalentTo(new[] { people[0] }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        switchable.OnNext(CreateExclusiveSource(people[1]));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(resourceOwner).IsSameReferenceAs(people[1]);
        await Assert.That(released).IsEquivalentTo(new[] { people[0] }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(people, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(switchable.HasObservers).IsFalse();
        await Assert.That(results.HasCompleted).IsFalse();

        IObservable<IChangeSet<Person, string>> CreateExclusiveSource(Person person) =>
            Observable.Create<IChangeSet<Person, string>>(observer =>
            {
                if (resourceOwner is not null)
                {
                    observer.OnError(new InvalidOperationException("The previous subscription still owns the resource."));
                    return Disposable.Empty;
                }

                resourceOwner = person;
                observer.OnNext(AddSubscriptionPerson(person));
                return Disposable.Create(() =>
                {
                    released.Add(person);
                    resourceOwner = null;
                });
            });
    }

    [Test]
    public async Task ReentrantSelectionDuringSynchronousAddReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        var people = CreateSubscriptionPeople(2);
        Person? resourceOwner = null;
        var released = new List<Person>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Do(changes =>
            {
                // Selects the replacement before the first Subscribe has returned its disposable.
                if (changes.Any(change => change.Reason is ChangeReason.Add && change.Key == people[0].Name))
                    switchable.OnNext(CreateExclusiveSource(people[1]));
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(CreateExclusiveSource(people[0]));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(resourceOwner).IsSameReferenceAs(people[1]);
        await Assert.That(released).IsEquivalentTo(new[] { people[0] }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Key, change.Current))).IsEquivalentTo(new[] { (ChangeReason.Add, people[0].Name, people[0]), (ChangeReason.Remove, people[0].Name, people[0]), (ChangeReason.Add, people[1].Name, people[1]) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(people, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        IObservable<IChangeSet<Person, string>> CreateExclusiveSource(Person person) =>
            Observable.Create<IChangeSet<Person, string>>(observer =>
            {
                if (resourceOwner is not null)
                {
                    observer.OnError(new InvalidOperationException("The previous subscription still owns the resource."));
                    return Disposable.Empty;
                }

                resourceOwner = person;
                observer.OnNext(AddSubscriptionPerson(person));
                return Disposable.Create(() =>
                {
                    released.Add(person);
                    resourceOwner = null;
                });
            });
    }

    [Test]
    public async Task ReentrantSelectionDuringTeardownReleasesPreviousSubscriptionBeforeAcquiringResource()
    {
        var people = CreateSubscriptionPeople(3);
        Person? resourceOwner = null;
        var released = new List<Person>();
        var supersededSubscriptions = 0;
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        var superseded = Observable.Defer(() =>
        {
            supersededSubscriptions++;
            return Observable.Never<IChangeSet<Person, string>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(CreateExclusiveSource(people[0], teardown: () => switchable.OnNext(CreateExclusiveSource(people[2]))));
        switchable.OnNext(superseded);

        await Assert.That(supersededSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[2] }.ToDictionary(person => person.Name));
        await Assert.That(resourceOwner).IsSameReferenceAs(people[2]);
        await Assert.That(released).IsEquivalentTo(new[] { people[0] }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Key, change.Current))).IsEquivalentTo(new[] { (ChangeReason.Add, people[0].Name, people[0]), (ChangeReason.Remove, people[0].Name, people[0]), (ChangeReason.Add, people[2].Name, people[2]) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        subscription.Dispose();

        await Assert.That(resourceOwner).IsNull();
        await Assert.That(released).IsEquivalentTo(new[] { people[0], people[2] }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        IObservable<IChangeSet<Person, string>> CreateExclusiveSource(Person person, Action? teardown = null) =>
            Observable.Create<IChangeSet<Person, string>>(observer =>
            {
                if (resourceOwner is not null)
                {
                    observer.OnError(new InvalidOperationException("The previous subscription still owns the resource."));
                    return Disposable.Empty;
                }

                resourceOwner = person;
                observer.OnNext(AddSubscriptionPerson(person));
                return Disposable.Create(() =>
                {
                    // Selects the replacement while this teardown still owns the resource.
                    teardown?.Invoke();
                    released.Add(person);
                    resourceOwner = null;
                });
            });
    }

    [Test]
    [Arguments(null)]
    [Arguments(NotificationKind.OnCompleted)]
    [Arguments(NotificationKind.OnError)]
    public async Task ReentrantSelectionDuringSynchronousAddKeepsLatestSubscription(NotificationKind? supersededTermination)
    {
        var people = CreateSubscriptionPeople(2);
        var supersededDisposals = 0;
        var supersededError = new InvalidOperationException("The superseded source failed before Subscribe returned.");
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var latest = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Person, string>>();
        var first = Observable.Create<IChangeSet<Person, string>>(observer =>
        {
            // The downstream callback selects latest before this Subscribe can return its disposable.
            observer.OnNext(AddSubscriptionPerson(people[0]));
            if (supersededTermination is NotificationKind.OnCompleted)
                observer.OnCompleted();
            else if (supersededTermination is NotificationKind.OnError)
                observer.OnError(supersededError);

            return Disposable.Create(() => supersededDisposals++);
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ChangeReason.Add && change.Key == people[0].Name))
                    switchable.OnNext(latest);
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(first);
        var latestRemainedSubscribed = latest.HasObservers;
        latest.OnNext(AddSubscriptionPerson(people[1]));
        switchable.OnCompleted();
        var completedBeforeLatest = results.HasCompleted;
        latest.OnCompleted();

        await Assert.That(results.Error).IsNull();
        await Assert.That(latestRemainedSubscribed).IsTrue();
        await Assert.That(supersededDisposals).IsEqualTo(1);
        await Assert.That(completedBeforeLatest).IsFalse();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Key, change.Current))).IsEquivalentTo(new[] { (ChangeReason.Add, people[0].Name, people[0]), (ChangeReason.Remove, people[0].Name, people[0]), (ChangeReason.Add, people[1].Name, people[1]) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringResetDoesNotSubscribeSupersededReplacement()
    {
        var people = CreateSubscriptionPeople(3);
        var supersededSubscriptions = 0;
        using var first = new SourceCache<Person, string>(person => person.Name);
        using var latest = new TestSourceCache<Person, string>(person => person.Name);
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        first.AddOrUpdate(people[0]);
        latest.AddOrUpdate(people[1]);
        var superseded = Observable.Defer(() =>
        {
            supersededSubscriptions++;
            return Observable.Never<IChangeSet<Person, string>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason is ChangeReason.Remove && change.Key == people[0].Name))
                    switchable.OnNext(latest.Connect());
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(superseded);
        latest.AddOrUpdate(people[2]);
        switchable.OnCompleted();
        latest.Complete();

        await Assert.That(supersededSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(people.Skip(1).ToDictionary(person => person.Name));
        await Assert.That(results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Key, change.Current))).IsEquivalentTo(new[] { (ChangeReason.Add, people[0].Name, people[0]), (ChangeReason.Remove, people[0].Name, people[0]), (ChangeReason.Add, people[1].Name, people[1]), (ChangeReason.Add, people[2].Name, people[2]) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringDisposalDoesNotSubscribeSupersededReplacement()
    {
        var people = CreateSubscriptionPeople(2);
        var firstDisposals = 0;
        var supersededSubscriptions = 0;
        using var latest = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Person, string>>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        var first = Observable.Create<IChangeSet<Person, string>>(observer =>
        {
            observer.OnNext(AddSubscriptionPerson(people[0]));
            return Disposable.Create(() =>
            {
                firstDisposals++;
                switchable.OnNext(latest);
            });
        });
        var superseded = Observable.Defer(() =>
        {
            supersededSubscriptions++;
            return Observable.Never<IChangeSet<Person, string>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(superseded);
        latest.OnNext(AddSubscriptionPerson(people[1]));
        switchable.OnCompleted();
        latest.OnCompleted();

        await Assert.That(firstDisposals).IsEqualTo(1);
        await Assert.That(supersededSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Key, change.Current))).IsEquivalentTo(new[] { (ChangeReason.Add, people[0].Name, people[0]), (ChangeReason.Remove, people[0].Name, people[0]), (ChangeReason.Add, people[1].Name, people[1]) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DisposalDuringResetDoesNotSubscribeReplacement()
    {
        var people = CreateSubscriptionPeople(2);
        var replacementSubscriptions = 0;
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(people[0]));
        using var replacementChanges = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Person, string>>();
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        var replacement = Observable.Defer(() =>
        {
            replacementSubscriptions++;
            return replacementChanges;
        });
        var results = new CacheItemRecordingObserver<Person, string>(Scheduler.Immediate);
        IObserver<IChangeSet<Person, string>> recorder = results;
        using var subscription = new ReactiveUI.Primitives.Disposables.OnceDisposable();
        subscription.Disposable = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Subscribe(
                changes =>
                {
                    // Record the reset before disposing, so the expected observable state is unambiguous.
                    recorder.OnNext(changes);
                    if (changes.Removes != 0)
                        subscription.Dispose();
                },
                recorder.OnError,
                recorder.OnCompleted);

        switchable.OnNext(first);
        switchable.OnNext(replacement);
        var notificationsAtDisposal = results.Notifications.ToArray();
        replacementChanges.OnNext(AddSubscriptionPerson(people[1]));
        replacementChanges.OnCompleted();

        await Assert.That(replacementSubscriptions).IsEqualTo(0);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(replacementChanges.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
        await Assert.That(results.Notifications.SequenceEqual(notificationsAtDisposal)).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsFalse();
    }

    [Test]
    public async Task OuterErrorDuringResetDoesNotSubscribeReplacement()
    {
        var person = CreateSubscriptionPeople(1)[0];
        var error = new InvalidOperationException("The outer source failed during reset.");
        var replacementSubscriptions = 0;
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(person));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        var replacement = Observable.Defer(() =>
        {
            replacementSubscriptions++;
            return Observable.Never<IChangeSet<Person, string>>();
        });
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(value => value.Name)
            .Do(changes =>
            {
                if (changes.Removes != 0)
                    switchable.OnError(error);
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(replacement);

        await Assert.That(replacementSubscriptions).IsEqualTo(0);
        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
    }

    [Test]
    public async Task OuterCompletionDuringResetWaitsForReplacement()
    {
        var people = CreateSubscriptionPeople(2);
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(people[0]));
        using var replacement = new TestSourceCache<Person, string>(person => person.Name);
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Do(changes =>
            {
                if (changes.Removes != 0)
                    switchable.OnCompleted();
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(first);
        switchable.OnNext(replacement.Connect());
        await Assert.That(results.HasCompleted).IsFalse();

        replacement.AddOrUpdate(people[1]);
        replacement.Complete();

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(first.HasObservers).IsFalse();
    }

    [Test]
    public async Task SynchronousOuterSubscriptionFailureReleasesActivatedInner()
    {
        var person = CreateSubscriptionPeople(1)[0];
        var error = new InvalidOperationException("The outer Subscribe failed after activating an inner source.");
        var innerDisposals = 0;
        using var inner = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(person));
        var source = RawAnonymousObservable.Create<IObservable<IChangeSet<Person, string>>>(observer =>
        {
            observer.OnNext(inner.Finally(() => innerDisposals++));
            throw error;
        });
        using var subscription = source.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(value => value.Name)
            .RecordCacheItems(out var results);

        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { person }.ToDictionary(value => value.Name));
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(inner.HasObservers).IsFalse();
        await Assert.That(innerDisposals).IsEqualTo(1);
    }

    [Test]
    public async Task SubscriptionsKeepIndependentCurrentSources()
    {
        var people = CreateSubscriptionPeople(2);
        using var first = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(people[0]));
        using var second = new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddSubscriptionPerson(people[1]));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        var switched = switchable.Switch();
        using var firstSubscription = switched
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var firstResults);
        using var secondSubscription = switched
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var secondResults);

        switchable.OnNext(first);
        firstSubscription.Dispose();
        switchable.OnNext(second);
        switchable.OnCompleted();
        second.OnCompleted();

        await Assert.That(firstResults.Error).IsNull();
        await Assert.That(firstResults.HasCompleted).IsFalse();
        await Assert.That(firstResults.RecordedItemsByKey).IsEquivalentTo(new[] { people[0] }.ToDictionary(person => person.Name));
        await Assert.That(secondResults.Error).IsNull();
        await Assert.That(secondResults.HasCompleted).IsTrue();
        await Assert.That(secondResults.RecordedItemsByKey).IsEquivalentTo(new[] { people[1] }.ToDictionary(person => person.Name));
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(second.HasObservers).IsFalse();
        await Assert.That(switchable.HasObservers).IsFalse();
    }

    private static IChangeSet<Person, string> AddSubscriptionPerson(Person person) =>
        new ChangeSet<Person, string> { new(ChangeReason.Add, person.Name, person) };

    private static Person[] CreateSubscriptionPeople(int count)
    {
        const int seed = 0x51A7;
        Console.WriteLine("Subscription lifetime data: seed={0}, count={1}", seed, count);
        return Fakers.Person.Clone().UseSeed(seed).Generate(count).ToArray();
    }
}
