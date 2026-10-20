// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

/// <summary>
/// An observer that writes into an operator's source while a notification caused by another
/// input of the same operator is being delivered must not receive the resulting notification
/// re-entrantly, inside the delivery that is still running. It must arrive afterwards, once.
/// </summary>
public sealed class ReentrantFeedbackDeliveryFixture
{
    private static readonly SortExpressionComparer<Person> ByAge = SortExpressionComparer<Person>.Ascending(p => p.Age);

    [Test]
    public async Task SortAndVirtualize()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.SortAndVirtualize(ByAge, requests), () => requests.OnNext(new VirtualRequest(0, 5)));
    }

    [Test]
    public async Task SortAndPage()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.SortAndPage(ByAge, requests), () => requests.OnNext(new PageRequest(1, 5)));
    }

    [Test]
    public async Task Virtualise()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(ByAge).Virtualise(requests), () => requests.OnNext(new VirtualRequest(0, 5)));
    }

    [Test]
    public async Task Page()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(ByAge).Page(requests), () => requests.OnNext(new PageRequest(1, 5)));
    }

    [Test]
    public async Task SortWithComparerChanges()
    {
        using var comparers = new ReactiveUI.Primitives.Signals.StateSignal<IComparer<Person>>(ByAge);
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(comparers), () => comparers.OnNext(SortExpressionComparer<Person>.Descending(p => p.Age)));
    }

    [Test]
    public async Task TransformWithForcedTransform()
    {
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        await AssertFeedbackIsDeliveredAfterwards(s => s.Transform((p, _) => new Person(p.Name, p.Age), force), () => force.OnNext(static (_, _) => true));
    }

    [Test]
    public async Task TransformAsyncWithForcedTransform()
    {
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        await AssertFeedbackIsDeliveredAfterwards(s => s.TransformAsync(p => Task.FromResult(new Person(p.Name, p.Age)), force), () => force.OnNext(static (_, _) => true));
    }

    [Test]
    public async Task BatchIf()
    {
        using var pause = new ReactiveUI.Primitives.Signals.StateSignal<bool>(true);
        await AssertFeedbackIsDeliveredAfterwards(s => s.BatchIf(pause, false, (TimeSpan?)null), () => pause.OnNext(false));
    }

    [Test]
    public async Task AutoRefresh()
    {
        Person? first = null;
        await AssertFeedbackIsDeliveredAfterwards(
            s => s.Do(changes => first ??= changes.First().Current).AutoRefresh(p => p.Age),
            () => first!.Age = 100);
    }

    [Test]
    public async Task GroupOnDynamicSelector()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age % 2);
        await AssertFeedbackIsNotReentrant(s => s.Group(selector), () => selector.OnNext((p, _) => p.Age % 3));
    }

    [Test]
    public async Task GroupOnWithRegrouper()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        Person? first = null;
        await AssertFeedbackIsNotReentrant(
            s => s.Do(changes => first ??= changes.First().Current).Group(p => p.Age, regrouper),
            () =>
            {
                first!.Age = 100;
                regrouper.OnNext(Unit.Default);
            });
    }

    [Test]
    public async Task GroupWithImmutableStateWithRegrouper()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        Person? first = null;
        await AssertFeedbackIsNotReentrant(
            s => s.Do(changes => first ??= changes.First().Current).GroupWithImmutableState(p => p.Age, regrouper),
            () =>
            {
                first!.Age = 100;
                regrouper.OnNext(Unit.Default);
            });
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task InnerJoin(bool triggerOnRight)
    {
        using var join = new JoinSources();
        await AssertCacheFeedback(
            () => join.Left.Connect().InnerJoin(join.Right.Connect(), static r => r.Name, static (l, r) => l.Name + l.Age + "+" + r.Age),
            triggerOnRight ? join.AddRightA : join.AddLeftB,
            triggerOnRight ? join.AddLeftB : join.AddRightA,
            triggerOnRight ? ("B", "B") : ("A", "A"));
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task LeftJoin(bool triggerOnRight)
    {
        using var join = new JoinSources();
        await AssertCacheFeedback(
            () => join.Left.Connect().LeftJoin(join.Right.Connect(), static r => r.Name, static (l, r) => l.Name + l.Age + "+" + (r.HasValue ? r.Value.Age : -1)),
            triggerOnRight ? join.AddRightA : join.AddLeftB,
            triggerOnRight ? join.AddLeftB : join.AddRightA,
            triggerOnRight ? "B" : "A");
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task RightJoin(bool triggerOnRight)
    {
        using var join = new JoinSources();
        await AssertCacheFeedback(
            () => join.Left.Connect().RightJoin(join.Right.Connect(), static r => r.Name, static (l, r) => (l.HasValue ? l.Value.Age : -1) + "+" + r.Name + r.Age),
            triggerOnRight ? join.AddRightA : join.AddLeftB,
            triggerOnRight ? join.AddLeftB : join.AddRightA,
            triggerOnRight ? "B" : "A");
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task FullJoin(bool triggerOnRight)
    {
        using var join = new JoinSources();
        await AssertCacheFeedback(
            () => join.Left.Connect().FullJoin(join.Right.Connect(), static r => r.Name, static (key, l, r) => key + (l.HasValue ? l.Value.Age : -1) + "+" + (r.HasValue ? r.Value.Age : -1)),
            triggerOnRight ? join.AddRightA : join.AddLeftB,
            triggerOnRight ? join.AddLeftB : join.AddRightA,
            triggerOnRight ? "B" : "A");
    }

    [Test]
    [Arguments("And")]
    [Arguments("Or")]
    [Arguments("Xor")]
    [Arguments("Except")]
    public async Task Combiner(string operation)
    {
        using var combined = new CombinerSources(operation);
        await AssertCacheFeedback(() => Combine(operation, combined.First.Connect(), combined.Second.Connect()), combined.Trigger, combined.Feedback, combined.FeedbackKey);
    }

    [Test]
    [Arguments("And")]
    [Arguments("Or")]
    [Arguments("Xor")]
    [Arguments("Except")]
    public async Task DynamicCombiner(string operation)
    {
        using var combined = new CombinerSources(operation);
        using var sources = new SourceList<IObservable<IChangeSet<Person, string>>>();
        sources.AddRange([combined.First.Connect(), combined.Second.Connect()]);
        await AssertCacheFeedback(() => CombineDynamic(operation, sources), combined.Trigger, combined.Feedback, combined.FeedbackKey);
    }

    [Test]
    public async Task DynamicCombinerFeedbackAddsSource()
    {
        using var first = new SourceCache<Person, string>(static p => p.Name);
        using var second = new SourceCache<Person, string>(static p => p.Name);
        using var third = new SourceCache<Person, string>(static p => p.Name);
        third.AddOrUpdate(new Person("C", 3));
        using var sources = new SourceList<IObservable<IChangeSet<Person, string>>>();
        sources.AddRange([first.Connect(), second.Connect()]);
        await AssertCacheFeedback(() => sources.Or(), () => second.AddOrUpdate(new Person("B", 2)), () => sources.Add(third.Connect()), "C");
    }

    [Test]
    public async Task MergeChangeSets()
    {
        using var first = new SourceCache<Person, string>(static p => p.Name);
        using var second = new SourceCache<Person, string>(static p => p.Name);
        await AssertCacheFeedback(
            () => new[] { first.Connect(), second.Connect() }.MergeChangeSets(),
            () => second.AddOrUpdate(new Person("B", 2)),
            () => first.AddOrUpdate(new Person("A", 1)),
            "A");
    }

    [Test]
    public async Task MergeChangeSetsFromObservableList()
    {
        using var first = new SourceCache<Person, string>(static p => p.Name);
        using var second = new SourceCache<Person, string>(static p => p.Name);
        using var third = new SourceCache<Person, string>(static p => p.Name);
        third.AddOrUpdate(new Person("C", 3));
        using var sources = new SourceList<IObservable<IChangeSet<Person, string>>>();
        sources.AddRange([first.Connect(), second.Connect()]);
        await AssertCacheFeedback(
            () => sources.MergeChangeSets(),
            () => second.AddOrUpdate(new Person("B", 2)),
            () =>
            {
                first.AddOrUpdate(new Person("A", 1));
                sources.Add(third.Connect());
            },
            "C");
    }

    [Test]
    public async Task MergeMany()
    {
        using var owners = new Owners();
        var received = await DeliverWithFeedback(owners.Source.Connect().MergeMany(static o => o.Value), () => owners.First.Value.OnNext(1), () => owners.Second.Value.OnNext(2));

        await Assert.That(received).Contains(1);
        await Assert.That(received[^1]).IsEqualTo(2);
    }

    [Test]
    public async Task MergeManyChangeSets()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(() => owners.Source.Connect().MergeManyChangeSets(static o => o.Children.Connect()), owners.AddFirstChild, owners.AddSecondChild, "B1");
    }

    [Test]
    public async Task MergeManyChangeSetsFeedbackIntoParent()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(() => owners.Source.Connect().MergeManyChangeSets(static o => o.Children.Connect()), owners.AddFirstChild, owners.AddThirdOwner, "C1");
    }

    [Test]
    public async Task MergeManyChangeSetsWithSourceComparer()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(
            () => owners.Source.Connect().MergeManyChangeSets(static o => o.Children.Connect(), SortExpressionComparer<FeedbackOwner>.Ascending(static o => o.Name)),
            owners.AddFirstChild,
            owners.AddSecondChild,
            "B1");
    }

    [Test]
    public async Task MergeManyListChangeSets()
    {
        using var owners = new Owners();
        var received = await DeliverWithFeedback(owners.Source.Connect().MergeManyChangeSets(static o => o.ListChildren.Connect()), owners.AddFirstChild, owners.AddSecondChild);
        var mirror = new List<Person>();
        foreach (var changes in received)
        {
            mirror.Clone(changes);
        }

        using var fresh = owners.Source.Connect().MergeManyChangeSets(static o => o.ListChildren.Connect()).AsObservableList();
        await Assert.That(fresh.Items.Select(static p => p.Name)).Contains("B1");
        await Assert.That(mirror).IsEquivalentTo(fresh.Items);
    }

    [Test]
    public async Task TransformOnObservable()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(() => owners.Source.Connect().TransformOnObservable(static o => o.Value), () => owners.First.Value.OnNext(1), () => owners.Second.Value.OnNext(2), "O2");
    }

    [Test]
    public async Task GroupOnObservable()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(
            () => owners.Source.Connect().GroupOnObservable(static o => o.Value),
            () => owners.First.Value.OnNext(1),
            () => owners.Second.Value.OnNext(2),
            2,
            compareValues: false);
    }

    [Test]
    public async Task TransformManyAsync()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(
            () => owners.Source.Connect().TransformManyAsync(static o => Task.FromResult<IObservableCache<Person, string>>(o.Children)),
            owners.AddFirstChild,
            owners.AddSecondChild,
            "B1");
    }

    [Test]
    public async Task TransformMany()
    {
        using var owners = new Owners();
        await AssertCacheFeedback(() => owners.Source.Connect().TransformMany(static o => o.Collection, static p => p.Name), owners.AddFirstChild, owners.AddThirdOwner, "C1");
    }

    [Test]
    public async Task FilterOnObservable()
    {
        using var source = new SourceCache<Person, string>(static p => p.Name);
        var first = new Person("First", 1);
        source.AddOrUpdate([first, new Person("Second", 2)]);
        await AssertCacheFeedback(
            () => source.Connect().FilterOnObservable(static p => p.WhenValueChanged(static x => x.Age).Select(static age => age < 50)),
            () => first.Age = 100,
            () => source.AddOrUpdate(new Person("Feedback", 0)),
            "Feedback");
    }

    [Test]
    public async Task QueryWhenChangedWithItemTrigger()
    {
        using var source = new SourceCache<Person, string>(static p => p.Name);
        var first = new Person("First", 1);
        source.AddOrUpdate([first, new Person("Second", 2)]);
        var received = await DeliverWithFeedback(
            source.Connect().QueryWhenChanged(static p => p.WhenValueChanged(static x => x.Age, notifyOnInitialValue: false)).Select(static query => query.Keys.ToArray()),
            () => first.Age = 100,
            () => source.AddOrUpdate(new Person("Feedback", 0)));

        await Assert.That(received[^1]).Contains("Feedback");
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task TransformToTree(bool triggerOnPredicate)
    {
        using var source = new SourceCache<Person, string>(static p => p.Name);
        source.AddOrUpdate(Enumerable.Range(1, 10).Select(static i => new Person("Seed" + i, i)));
        using var predicates = new ReactiveUI.Primitives.Signals.StateSignal<Func<Node<Person, string>, bool>>(static n => n.Item.Age < 5);
        void ChangePredicate() => predicates.OnNext(static n => n.Item.Age < 8);
        void AddFeedback() => source.AddOrUpdate(new Person("Feedback", 0));
        void AddTrigger() => source.AddOrUpdate(new Person("Trigger", 0));

        await AssertCacheFeedback(
            () => source.Connect().TransformToTree(static _ => "none", predicates),
            triggerOnPredicate ? ChangePredicate : AddTrigger,
            triggerOnPredicate ? AddFeedback : ChangePredicate,
            triggerOnPredicate ? "Feedback" : "Seed7",
            compareValues: false);
    }

    [Test]
    public async Task FeedbackPendingWhenDisposedIsNotDelivered()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 3));
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate(Enumerable.Range(1, 10).Select(i => new Person("Seed" + i, i)));

        var armed = false;
        var deliveredAfterDispose = 0;
        var disposed = false;
        IDisposable? subscription = null;
        subscription = source.Connect().SortAndVirtualize(ByAge, requests).Subscribe(_ =>
        {
            if (disposed)
            {
                deliveredAfterDispose++;
            }

            if (armed)
            {
                armed = false;

                // Held back until this delivery returns, by which time the subscription is disposed.
                source.AddOrUpdate(new Person("Feedback", 0));
                subscription!.Dispose();
                disposed = true;
            }
        });

        armed = true;
        requests.OnNext(new VirtualRequest(0, 5));

        await Assert.That(disposed).IsTrue();
        await Assert.That(deliveredAfterDispose).IsEqualTo(0);
    }

    private static async Task AssertFeedbackIsNotReentrant<T>(
        Func<IObservable<IChangeSet<Person, string>>, IObservable<T>> pipeline,
        Action triggerFromOtherInput)
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate(Enumerable.Range(1, 10).Select(i => new Person("Seed" + i, i)));

        var errors = new ConcurrentQueue<Exception>();
        var armed = false;
        var received = 0;

        using var subscription = pipeline(source.Connect())
            .ValidateSynchronization()
            .RecordErrors(errors)
            .Subscribe(_ =>
            {
                received++;
                if (armed)
                {
                    armed = false;
                    // A new group, so the grouping publishes a change for it.
                    source.AddOrUpdate(new Person("Feedback", -1));
                }
            });

        var before = received;
        armed = true;
        triggerFromOtherInput();

        await Assert.That(armed).IsFalse().Because("the trigger must produce a notification");
        await Assert.That(errors).IsEmpty();
        await Assert.That(received).IsGreaterThan(before + 1).Because("the feedback must be delivered too");
    }

    private static async Task AssertFeedbackIsDeliveredAfterwards<TChanges>(
        Func<IObservable<IChangeSet<Person, string>>, IObservable<TChanges>> pipeline,
        Action triggerFromOtherInput)
        where TChanges : IChangeSet<Person, string>
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate(Enumerable.Range(1, 10).Select(i => new Person("Seed" + i, i)));

        var errors = new ConcurrentQueue<Exception>();
        var inconsistencies = new List<string>();
        var mirror = new Dictionary<string, Person>();
        var armed = false;

        using var subscription = pipeline(source.Connect())
            .ValidateSynchronization()
            .RecordErrors(errors)
            .Subscribe(changes =>
            {
                foreach (var change in changes)
                {
                    switch (change.Reason)
                    {
                        case ChangeReason.Add when !mirror.TryAdd(change.Key, change.Current):
                            inconsistencies.Add("duplicate add " + change.Key);
                            break;
                        case ChangeReason.Remove when !mirror.Remove(change.Key):
                            inconsistencies.Add("remove of missing " + change.Key);
                            break;
                        case ChangeReason.Update:
                            mirror[change.Key] = change.Current;
                            break;
                    }
                }

                if (armed)
                {
                    armed = false;
                    source.AddOrUpdate(new Person("Feedback", 0));
                }
            });

        armed = true;
        triggerFromOtherInput();

        await Assert.That(armed).IsFalse().Because("the trigger must produce a notification");
        await Assert.That(errors).IsEmpty();
        await Assert.That(inconsistencies).IsEmpty();
        await Assert.That(mirror.ContainsKey("Feedback")).IsTrue();

        // The delivered changes add up to what a fresh subscription sees.
        var expected = new Dictionary<string, Person>();
        using (pipeline(source.Connect()).Subscribe(changes =>
        {
            foreach (var change in changes)
            {
                if (change.Reason == ChangeReason.Remove)
                    expected.Remove(change.Key);
                else
                    expected[change.Key] = change.Current;
            }
        }))
        {
        }

        await Assert.That(mirror.Keys).IsEquivalentTo(expected.Keys);
    }

    private static IObservable<IChangeSet<Person, string>> Combine(string operation, IObservable<IChangeSet<Person, string>> first, IObservable<IChangeSet<Person, string>> second) => operation switch
    {
        "And" => ObservableCacheEx.And(first, second),
        "Or" => first.Or(second),
        "Xor" => first.Xor(second),
        "Except" => first.Except(second),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static IObservable<IChangeSet<Person, string>> CombineDynamic(string operation, IObservableList<IObservable<IChangeSet<Person, string>>> sources) => operation switch
    {
        "And" => sources.And(),
        "Or" => sources.Or(),
        "Xor" => sources.Xor(),
        "Except" => sources.Except(),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    /// <summary>
    /// Subscribes, runs <paramref name="trigger"/>, and runs <paramref name="feedback"/> from inside the first
    /// notification that follows. Asserts that no notification overlapped another and that the feedback was
    /// delivered too, and returns every notification received.
    /// </summary>
    private static async Task<List<T>> DeliverWithFeedback<T>(IObservable<T> pipeline, Action trigger, Action feedback)
    {
        var errors = new ConcurrentQueue<Exception>();
        var received = new List<T>();
        var armed = false;
        int before;

        using (pipeline
            .ValidateSynchronization()
            .RecordErrors(errors)
            .Subscribe(value =>
            {
                received.Add(value);
                if (armed)
                {
                    armed = false;
                    feedback();
                }
            }))
        {
            before = received.Count;
            armed = true;
            trigger();
        }

        await Assert.That(armed).IsFalse().Because("the trigger must produce a notification");
        await Assert.That(errors).IsEmpty();
        await Assert.That(received.Count).IsGreaterThan(before + 1).Because("the feedback must be delivered too");
        return received;
    }

    /// <summary>
    /// As <see cref="DeliverWithFeedback{T}"/>, and asserts that the delivered changes are consistent and add up
    /// to what a fresh subscription sees, including <paramref name="feedbackKey"/>.
    /// </summary>
    private static async Task AssertCacheFeedback<TObject, TKey>(
        Func<IObservable<IChangeSet<TObject, TKey>>> pipeline,
        Action trigger,
        Action feedback,
        TKey feedbackKey,
        bool compareValues = true)
        where TObject : notnull
        where TKey : notnull
    {
        var received = await DeliverWithFeedback(pipeline(), trigger, feedback);

        var inconsistencies = new List<string>();
        var mirror = new Dictionary<TKey, TObject>();
        foreach (var change in received.SelectMany(static changes => changes))
        {
            switch (change.Reason)
            {
                case ChangeReason.Add when !mirror.TryAdd(change.Key, change.Current):
                    inconsistencies.Add("duplicate add " + change.Key);
                    break;
                case ChangeReason.Remove when !mirror.Remove(change.Key):
                    inconsistencies.Add("remove of missing " + change.Key);
                    break;
                case ChangeReason.Update:
                    mirror[change.Key] = change.Current;
                    break;
            }
        }

        using var fresh = pipeline().AsObservableCache();

        await Assert.That(inconsistencies).IsEmpty();
        await Assert.That(fresh.Keys).Contains(feedbackKey);
        await Assert.That(mirror.Keys).IsEquivalentTo(fresh.Keys);
        if (compareValues)
        {
            foreach (var pair in fresh.KeyValues)
            {
                await Assert.That(mirror[pair.Key]).IsEqualTo(pair.Value);
            }
        }
    }

    /// <summary>Two caches to join on name: the left holds A, the right holds B, so neither joins yet.</summary>
    private sealed class JoinSources : IDisposable
    {
        public JoinSources()
        {
            Left.AddOrUpdate(new Person("A", 1));
            Right.AddOrUpdate(new Person("B", 20));
        }

        public SourceCache<Person, string> Left { get; } = new(static p => p.Name);

        public SourceCache<Person, string> Right { get; } = new(static p => p.Name);

        public void AddRightA() => Right.AddOrUpdate(new Person("A", 10));

        public void AddLeftB() => Left.AddOrUpdate(new Person("B", 2));

        public void Dispose()
        {
            Left.Dispose();
            Right.Dispose();
        }
    }

    /// <summary>
    /// Two caches to combine: the first holds A, the second C. The trigger writes into the second and the
    /// feedback into the first, each changing the combined result.
    /// </summary>
    private sealed class CombinerSources : IDisposable
    {
        private readonly string _operation;

        public CombinerSources(string operation)
        {
            _operation = operation;
            First.AddOrUpdate(new Person("A", 1));
            Second.AddOrUpdate(new Person("C", 3));
        }

        public SourceCache<Person, string> First { get; } = new(static p => p.Name);

        public SourceCache<Person, string> Second { get; } = new(static p => p.Name);

        public string FeedbackKey => _operation == "And" ? "C" : "D";

        public void Trigger() => Second.AddOrUpdate(_operation is "And" or "Except" ? new Person("A", 1) : new Person("B", 2));

        public void Feedback() => First.AddOrUpdate(_operation == "And" ? new Person("C", 3) : new Person("D", 4));

        public void Dispose()
        {
            First.Dispose();
            Second.Dispose();
        }
    }

    /// <summary>A cache of two owners, each with a child.</summary>
    private sealed class Owners : IDisposable
    {
        public Owners() => Source.AddOrUpdate([First, Second]);

        public SourceCache<FeedbackOwner, string> Source { get; } = new(static o => o.Name);

        public FeedbackOwner First { get; } = new("O1", new Person("A0", 0));

        public FeedbackOwner Second { get; } = new("O2", new Person("B0", 0));

        public void AddFirstChild() => First.Add(new Person("A1", 1));

        public void AddSecondChild() => Second.Add(new Person("B1", 1));

        public void AddThirdOwner() => Source.AddOrUpdate(new FeedbackOwner("O3", new Person("C1", 1)));

        public void Dispose()
        {
            foreach (var owner in Source.Items)
            {
                owner.Dispose();
            }

            Source.Dispose();
        }
    }
}
