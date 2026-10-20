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
}
