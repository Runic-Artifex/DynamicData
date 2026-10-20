#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

// Switching between populated sources publishes the removals and the new source's snapshot as one changeset,
// so subscribers never observe the empty cache in between.
public class SwitchHandoffFixture
{
    private static readonly Exception HandoffError = new InvalidOperationException("The new source failed during the handoff.");

    [Test]
    public async Task HandsOffPopulatedSourcesInOneChangeset()
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        var bobAgain = new Person("Bob", 20);
        var carol = new Person("Carol", 3);
        using var first = new SourceCache<Person, string>(person => person.Name);
        using var second = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(new[] { alice, bob });
        second.AddOrUpdate(new[] { bobAgain, carol });
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(first.Connect());
        var bound = new ObservableCollectionExtended<Person>();
        var counts = new List<int>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Bind(bound)
            .Do(_ => counts.Add(bound.Count))
            .RecordCacheItems(out var results);

        switchable.OnNext(second.Connect());

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => (change.Reason, change.Key, change.Current, change.Previous.HasValue ? change.Previous.Value : null)))
            .IsEquivalentTo(new (ChangeReason, string, Person, Person?)[] { (ChangeReason.Remove, "Alice", alice, null), (ChangeReason.Update, "Bob", bobAgain, bob), (ChangeReason.Add, "Carol", carol, null) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { bobAgain, carol }.ToDictionary(person => person.Name));
        await Assert.That(bound).IsEquivalentTo(new[] { bobAgain, carol });
        await Assert.That(counts).DoesNotContain(0);

        second.AddOrUpdate(new Person("Dave", 4));
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(3);
        await Assert.That(results.RecordedItemsByKey.Count).IsEqualTo(3);
    }

    [Test]
    public async Task FoldsOnlyTheFirstSynchronousChangeset()
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        var carol = new Person("Carol", 3);
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(Observable.Return(AddPeople(alice)).Concat(Observable.Never<IChangeSet<Person, string>>()));
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(Observable.Create<IChangeSet<Person, string>>(observer =>
        {
            observer.OnNext(AddPeople(bob));
            observer.OnNext(AddPeople(carol));
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Skip(1).Select(changes => changes.Select(change => (change.Reason, change.Key)).ToArray()).ToArray())
            .IsEquivalentTo(new[] { new[] { (ChangeReason.Remove, "Alice"), (ChangeReason.Add, "Bob") }, new[] { (ChangeReason.Add, "Carol") } }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { bob, carol }.ToDictionary(person => person.Name));
    }

    [Test]
    public async Task HandoffWaitsPastAnInitialEmptyChangeset()
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(new ReactiveUI.Primitives.Signals.StateSignal<IChangeSet<Person, string>>(AddPeople(alice)));
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(Observable.Create<IChangeSet<Person, string>>(observer =>
        {
            observer.OnNext(ChangeSet<Person, string>.Empty);
            observer.OnNext(AddPeople(bob));
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => (change.Reason, change.Key)))
            .IsEquivalentTo(new[] { (ChangeReason.Remove, "Alice"), (ChangeReason.Add, "Bob") }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DelayedSourcePublishesRemovalsFirst()
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        using var first = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(alice);
        using var delayed = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Person, string>>();
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(delayed);

        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => (change.Reason, change.Key))).IsEquivalentTo(new[] { (ChangeReason.Remove, "Alice") });
        await Assert.That(results.RecordedItemsByKey).IsEmpty();

        delayed.OnNext(AddPeople(bob));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(3);
        await Assert.That(results.RecordedChangeSets[2].Select(change => (change.Reason, change.Key))).IsEquivalentTo(new[] { (ChangeReason.Add, "Bob") });
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { bob }.ToDictionary(person => person.Name));
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task EmptySourcePublishesOnlyTheRemovals(bool emitsInitialEmptyChangeset)
    {
        var alice = new Person("Alice", 1);
        using var first = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(alice);
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(emitsInitialEmptyChangeset
            ? Observable.Return(ChangeSet<Person, string>.Empty).Concat(Observable.Never<IChangeSet<Person, string>>())
            : Observable.Never<IChangeSet<Person, string>>());

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => (change.Reason, change.Key))).IsEquivalentTo(new[] { (ChangeReason.Remove, "Alice") });
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ErrorDuringHandoffFollowsWhatWasOwed(bool emitsSnapshotFirst)
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        using var first = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(alice);
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(emitsSnapshotFirst
            ? Observable.Return(AddPeople(bob)).Concat(Observable.Throw<IChangeSet<Person, string>>(HandoffError))
            : Observable.Throw<IChangeSet<Person, string>>(HandoffError));

        await Assert.That(results.Error).IsSameReferenceAs(HandoffError);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        await Assert.That(results.RecordedChangeSets[1].Select(change => (change.Reason, change.Key)))
            .IsEquivalentTo(emitsSnapshotFirst ? new[] { (ChangeReason.Remove, "Alice"), (ChangeReason.Add, "Bob") } : new[] { (ChangeReason.Remove, "Alice") }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task CompletionDuringHandoffPublishesRemovalsFirst()
    {
        using var first = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(new Person("Alice", 1));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(Observable.Create<IChangeSet<Person, string>>(observer =>
        {
            // Both sequences end before the new source has delivered anything.
            switchable.OnCompleted();
            observer.OnCompleted();
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OuterErrorDuringHandoffPublishesRemovalsFirst()
    {
        var error = new InvalidOperationException("The outer source failed during the handoff.");
        using var first = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(new Person("Alice", 1));
        using var switchable = new ReactiveUI.Primitives.Signals.Signal<IObservable<IChangeSet<Person, string>>>();
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .RecordCacheItems(out var results);

        switchable.OnNext(first.Connect());
        switchable.OnNext(Observable.Create<IChangeSet<Person, string>>(_ =>
        {
            switchable.OnError(error);
            return Disposable.Empty;
        }));

        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.RecordedItemsByKey).IsEmpty();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReentrantSelectionDuringHandoffKeepsLatestSource()
    {
        var alice = new Person("Alice", 1);
        var bob = new Person("Bob", 2);
        var carol = new Person("Carol", 3);
        var events = new List<string>();
        using var first = new SourceCache<Person, string>(person => person.Name);
        using var second = new SourceCache<Person, string>(person => person.Name);
        using var latest = new SourceCache<Person, string>(person => person.Name);
        first.AddOrUpdate(alice);
        second.AddOrUpdate(bob);
        latest.AddOrUpdate(carol);
        using var switchable = new ReactiveUI.Primitives.Signals.StateSignal<IObservable<IChangeSet<Person, string>>>(first.Connect());
        using var subscription = switchable.Switch()
            .ValidateSynchronization()
            .ValidateChangeSets(person => person.Name)
            .Do(changes =>
            {
                // Selects the latest source while the handoff batch from the second source is being delivered.
                if (changes.Any(change => change.Key == "Bob" && change.Reason is ChangeReason.Add))
                    switchable.OnNext(Track("latest", latest.Connect()));
            })
            .RecordCacheItems(out var results);

        switchable.OnNext(Track("second", second.Connect()));

        await Assert.That(results.Error).IsNull();
        await Assert.That(events).IsEquivalentTo(new[] { "second subscribed", "second released", "latest subscribed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.Skip(1).Select(changes => changes.Select(change => (change.Reason, change.Key)).ToArray()).ToArray())
            .IsEquivalentTo(new[] { new[] { (ChangeReason.Remove, "Alice"), (ChangeReason.Add, "Bob") }, new[] { (ChangeReason.Remove, "Bob"), (ChangeReason.Add, "Carol") } }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new[] { carol }.ToDictionary(person => person.Name));

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

    private static IChangeSet<Person, string> AddPeople(params Person[] people) =>
        new ChangeSet<Person, string>(people.Select(person => new Change<Person, string>(ChangeReason.Add, person.Name, person)));
}
