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
}
