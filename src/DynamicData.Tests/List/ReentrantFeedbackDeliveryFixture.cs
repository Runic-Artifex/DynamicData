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

namespace DynamicData.Tests.List;

/// <summary>
/// An observer that writes into an operator's source while a notification caused by another
/// input of the same operator is being delivered must not receive the resulting notification
/// re-entrantly, inside the delivery that is still running. It must arrive afterwards, once.
/// </summary>
public sealed class ReentrantFeedbackDeliveryFixture
{
    private static readonly SortExpressionComparer<Person> ByAge = SortExpressionComparer<Person>.Ascending(p => p.Age);

    [Test]
    public async Task SortWithComparerChanges()
    {
        using var comparers = new ReactiveUI.Primitives.Signals.StateSignal<IComparer<Person>>(ByAge);
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(comparers), () => comparers.OnNext(SortExpressionComparer<Person>.Descending(p => p.Age)));
    }

    [Test]
    public async Task Page()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(ByAge).Page(requests), () => requests.OnNext(new PageRequest(1, 5)));
    }

    [Test]
    public async Task Virtualise()
    {
        using var requests = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 3));
        await AssertFeedbackIsDeliveredAfterwards(s => s.Sort(ByAge).Virtualise(requests), () => requests.OnNext(new VirtualRequest(0, 5)));
    }

    [Test]
    public async Task FilterWithPredicateChanges()
    {
        using var predicates = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, bool>>(p => p.Age < 5);
        await AssertFeedbackIsDeliveredAfterwards(s => s.Filter(predicates), () => predicates.OnNext(p => p.Age < 8));
    }

    [Test]
    public async Task BufferIf()
    {
        var scheduler = new TestScheduler();
        using var pause = new ReactiveUI.Primitives.Signals.StateSignal<bool>(true);
        await AssertFeedbackIsDeliveredAfterwards(
            s => s.BufferIf(pause, true, scheduler),
            () =>
            {
                pause.OnNext(false);
                scheduler.AdvanceBy(1);
            },
            settle: () => scheduler.AdvanceBy(1));
    }

    [Test]
    public async Task AutoRefresh()
    {
        Person? first = null;
        await AssertFeedbackIsDeliveredAfterwards(
            s => s.Do(changes => first ??= changes.First().Item.Current ?? changes.First().Range.First()).AutoRefresh(p => p.Age),
            () => first!.Age = 100);
    }

    [Test]
    public async Task FilterWithPredicateState()
    {
        using var state = new ReactiveUI.Primitives.Signals.StateSignal<int>(5);
        await AssertFeedbackIsDeliveredAfterwards(s => s.Filter(state, static (limit, p) => p.Age < limit), () => state.OnNext(8));
    }

    [Test]
    public async Task FilterOnObservable()
    {
        Person? first = null;
        await AssertFeedbackIsDeliveredAfterwards(
            s => s.Do(changes => first ??= changes.First().Item.Current ?? changes.First().Range.First())
                .FilterOnObservable(static p => p.WhenValueChanged(static x => x.Age).Select(static age => age < 50)),
            () => first!.Age = 100);
    }

    [Test]
    public async Task GroupOnWithRegrouper()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceList<Person>();
        var first = new Person("First", 1);
        source.AddRange([first, new Person("Second", 2)]);
        await AssertListFeedback(
            () => source.Connect().GroupOn(static p => p.Age, regrouper),
            () =>
            {
                first.Age = 100;
                regrouper.OnNext(Unit.Default);
            },
            () => source.Add(new Person("Feedback", -1)),
            static g => g.GroupKey,
            -1);
    }

    [Test]
    public async Task GroupWithImmutableStateWithRegrouper()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceList<Person>();
        var first = new Person("First", 1);
        source.AddRange([first, new Person("Second", 2)]);
        await AssertListFeedback(
            () => source.Connect().GroupWithImmutableState(static p => p.Age, regrouper),
            () =>
            {
                first.Age = 100;
                regrouper.OnNext(Unit.Default);
            },
            () => source.Add(new Person("Feedback", -1)),
            static g => g.Key,
            -1);
    }

    [Test]
    [Arguments("And")]
    [Arguments("Or")]
    [Arguments("Xor")]
    [Arguments("Except")]
    public async Task Combiner(string operation)
    {
        using var combined = new CombinerSources(operation);
        await AssertListFeedback(() => Combine(operation, combined.First.Connect(), combined.Second.Connect()), combined.Trigger, combined.Feedback, static p => p.Name, combined.FeedbackName);
    }

    [Test]
    [Arguments("And")]
    [Arguments("Or")]
    [Arguments("Xor")]
    [Arguments("Except")]
    public async Task DynamicCombiner(string operation)
    {
        using var combined = new CombinerSources(operation);
        using var sources = new SourceList<IObservable<IChangeSet<Person>>>();
        sources.AddRange([combined.First.Connect(), combined.Second.Connect()]);
        await AssertListFeedback(() => CombineDynamic(operation, sources), combined.Trigger, combined.Feedback, static p => p.Name, combined.FeedbackName);
    }

    [Test]
    public async Task DynamicCombinerFeedbackAddsSource()
    {
        using var first = new SourceList<Person>();
        using var second = new SourceList<Person>();
        using var third = new SourceList<Person>();
        third.Add(new Person("C", 3));
        using var sources = new SourceList<IObservable<IChangeSet<Person>>>();
        sources.AddRange([first.Connect(), second.Connect()]);
        await AssertListFeedback(() => sources.Or(), () => second.Add(new Person("B", 2)), () => sources.Add(third.Connect()), static p => p.Name, "C");
    }

    [Test]
    public async Task MergeChangeSets()
    {
        using var first = new SourceList<Person>();
        using var second = new SourceList<Person>();
        await AssertListFeedback(
            () => new[] { first.Connect(), second.Connect() }.MergeChangeSets(),
            () => second.Add(new Person("B", 2)),
            () => first.Add(new Person("A", 1)),
            static p => p.Name,
            "A");
    }

    [Test]
    public async Task MergeChangeSetsFromObservableList()
    {
        using var first = new SourceList<Person>();
        using var second = new SourceList<Person>();
        using var third = new SourceList<Person>();
        third.Add(new Person("C", 3));
        using var sources = new SourceList<IObservable<IChangeSet<Person>>>();
        sources.AddRange([first.Connect(), second.Connect()]);
        await AssertListFeedback(
            () => sources.MergeChangeSets(),
            () => second.Add(new Person("B", 2)),
            () =>
            {
                first.Add(new Person("A", 1));
                sources.Add(third.Connect());
            },
            static p => p.Name,
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
    public async Task MergeManyFeedbackIntoSource()
    {
        using var owners = new Owners();
        var third = new FeedbackOwner("O3");
        third.Value.OnNext(3);
        var received = await DeliverWithFeedback(owners.Source.Connect().MergeMany(static o => o.Value), () => owners.First.Value.OnNext(1), () => owners.Source.Add(third));

        await Assert.That(received).Contains(1);
        await Assert.That(received[^1]).IsEqualTo(3);
    }

    [Test]
    public async Task MergeManyChangeSets()
    {
        using var owners = new Owners();
        await AssertListFeedback(() => owners.Source.Connect().MergeManyChangeSets(static o => o.ListChildren.Connect()), owners.AddFirstChild, owners.AddSecondChild, static p => p.Name, "B1");
    }

    [Test]
    public async Task MergeManyChangeSetsFeedbackIntoParent()
    {
        using var owners = new Owners();
        await AssertListFeedback(() => owners.Source.Connect().MergeManyChangeSets(static o => o.ListChildren.Connect()), owners.AddFirstChild, owners.AddThirdOwner, static p => p.Name, "C1");
    }

    [Test]
    public async Task MergeManyCacheChangeSets()
    {
        using var owners = new Owners();
        var received = await DeliverWithFeedback(owners.Source.Connect().MergeManyChangeSets(static o => o.Children.Connect()), owners.AddFirstChild, owners.AddSecondChild);
        var mirror = new Dictionary<string, Person>();
        foreach (var change in received.SelectMany(static changes => changes))
        {
            if (change.Reason == ChangeReason.Remove)
                mirror.Remove(change.Key);
            else
                mirror[change.Key] = change.Current;
        }

        using var fresh = owners.Source.Connect().MergeManyChangeSets(static o => o.Children.Connect()).AsObservableCache();
        await Assert.That(fresh.Keys).Contains("B1");
        await Assert.That(mirror.Keys).IsEquivalentTo(fresh.Keys);
    }

    [Test]
    public async Task TransformMany()
    {
        using var owners = new Owners();
        await AssertListFeedback(() => owners.Source.Connect().TransformMany(static o => o.Collection), owners.AddFirstChild, owners.AddThirdOwner, static p => p.Name, "C1");
    }

    [Test]
    public async Task FeedbackPendingWhenDisposedIsNotDelivered()
    {
        using var comparers = new ReactiveUI.Primitives.Signals.StateSignal<IComparer<Person>>(ByAge);
        using var source = new SourceList<Person>();
        source.AddRange(Enumerable.Range(1, 10).Select(i => new Person("Seed" + i, i)));

        var armed = false;
        var deliveredAfterDispose = 0;
        var disposed = false;
        IDisposable? subscription = null;
        subscription = source.Connect().Sort(comparers).Subscribe(_ =>
        {
            if (disposed)
            {
                deliveredAfterDispose++;
            }

            if (armed)
            {
                armed = false;

                // Held back until this delivery returns, by which time the subscription is disposed.
                source.Add(new Person("Feedback", 0));
                subscription!.Dispose();
                disposed = true;
            }
        });

        armed = true;
        comparers.OnNext(SortExpressionComparer<Person>.Descending(p => p.Age));

        await Assert.That(disposed).IsTrue();
        await Assert.That(deliveredAfterDispose).IsEqualTo(0);
    }

    private static async Task AssertFeedbackIsDeliveredAfterwards<TChanges>(
        Func<IObservable<IChangeSet<Person>>, IObservable<TChanges>> pipeline,
        Action triggerFromOtherInput,
        Action? settle = null)
        where TChanges : IChangeSet<Person>
    {
        using var source = new SourceList<Person>();
        source.AddRange(Enumerable.Range(1, 10).Select(i => new Person("Seed" + i, i)));

        var errors = new ConcurrentQueue<Exception>();
        var mirror = new List<Person>();
        var armed = false;

        using var subscription = pipeline(source.Connect())
            .ValidateSynchronization()
            .RecordErrors(errors)
            .Subscribe(changes =>
            {
                mirror.Clone(changes);
                if (armed)
                {
                    armed = false;
                    source.Add(new Person("Feedback", 0));
                }
            });

        armed = true;
        triggerFromOtherInput();
        settle?.Invoke();

        await Assert.That(armed).IsFalse().Because("the trigger must produce a notification");
        await Assert.That(errors).IsEmpty();

        string[] expected;
        using (var fresh = pipeline(source.Connect()).Select(static changes => (IChangeSet<Person>)changes).AsObservableList())
        {
            settle?.Invoke();
            expected = fresh.Items.Select(p => p.Name).ToArray();
        }

        await Assert.That(expected).Contains("Feedback");
        await Assert.That(mirror.Select(p => p.Name).ToArray()).IsEquivalentTo(expected);
    }

    private static IObservable<IChangeSet<Person>> Combine(string operation, IObservable<IChangeSet<Person>> first, IObservable<IChangeSet<Person>> second) => operation switch
    {
        "And" => ObservableListEx.And(first, second),
        "Or" => first.Or(second),
        "Xor" => first.Xor(second),
        "Except" => first.Except(second),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static IObservable<IChangeSet<Person>> CombineDynamic(string operation, IObservableList<IObservable<IChangeSet<Person>>> sources) => operation switch
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
    /// As <see cref="DeliverWithFeedback{T}"/>, and asserts that the delivered changes add up to what a fresh
    /// subscription sees, compared by <paramref name="identity"/>, including <paramref name="feedbackIdentity"/>.
    /// </summary>
    private static async Task AssertListFeedback<T, TIdentity>(
        Func<IObservable<IChangeSet<T>>> pipeline,
        Action trigger,
        Action feedback,
        Func<T, TIdentity> identity,
        TIdentity feedbackIdentity)
        where T : notnull
    {
        var received = await DeliverWithFeedback(pipeline(), trigger, feedback);

        var mirror = new List<T>();
        foreach (var changes in received)
        {
            mirror.Clone(changes);
        }

        using var fresh = pipeline().AsObservableList();
        var expected = fresh.Items.Select(identity).ToArray();

        await Assert.That(expected).Contains(feedbackIdentity);
        await Assert.That(mirror.Select(identity).ToArray()).IsEquivalentTo(expected);
    }

    /// <summary>
    /// Two lists to combine: the first holds A, the second C. The trigger writes into the second and the
    /// feedback into the first, each changing the combined result.
    /// </summary>
    private sealed class CombinerSources : IDisposable
    {
        private static readonly Person A = new("A", 1);
        private static readonly Person C = new("C", 3);
        private readonly string _operation;

        public CombinerSources(string operation)
        {
            _operation = operation;
            First.Add(A);
            Second.Add(C);
        }

        public SourceList<Person> First { get; } = new();

        public SourceList<Person> Second { get; } = new();

        public string FeedbackName => _operation == "And" ? "C" : "D";

        public void Trigger() => Second.Add(_operation is "And" or "Except" ? A : new Person("B", 2));

        public void Feedback() => First.Add(_operation == "And" ? C : new Person("D", 4));

        public void Dispose()
        {
            First.Dispose();
            Second.Dispose();
        }
    }

    /// <summary>A list of two owners, each with a child.</summary>
    private sealed class Owners : IDisposable
    {
        public Owners() => Source.AddRange([First, Second]);

        public SourceList<FeedbackOwner> Source { get; } = new();

        public FeedbackOwner First { get; } = new("O1", new Person("A0", 0));

        public FeedbackOwner Second { get; } = new("O2", new Person("B0", 0));

        public void AddFirstChild() => First.Add(new Person("A1", 1));

        public void AddSecondChild() => Second.Add(new Person("B1", 1));

        public void AddThirdOwner() => Source.Add(new FeedbackOwner("O3", new Person("C1", 1)));

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
