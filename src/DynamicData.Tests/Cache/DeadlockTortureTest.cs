// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

/// <summary>
/// Deadlock torture test. Every dangerous operator (one that holds a lock during
/// downstream delivery) is wired into a bidirectional cross-cache pipeline.
/// Two threads write simultaneously, creating the ABBA lock cycle:
///   Thread A: sourceA._locker -> operator lock -> PopulateInto -> sourceB._locker
///   Thread B: sourceB._locker -> operator lock -> PopulateInto -> sourceA._locker
///
/// On main (Synchronize(lock)): deadlocks reliably within seconds.
/// On the PR branch (SynchronizeSafe queue-drain): no deadlock possible.
/// </summary>
public sealed class DeadlockTortureTest
    : IntegrationTestFixtureBase
{
    private const int ItemCount = 200;
    private const int Iterations = 50;
    private const int TimeoutSeconds = 60;

    private static async Task<bool> RunBidirectionalDeadlockTest(
        Func<IObservable<IChangeSet<Person, string>>, IObservable<IChangeSet<Person, string>>> pipeline,
        Action? subjectPusher = null,
        int iterations = Iterations,
        Propagation propagation = Propagation.All)
    {
        for (var iter = 0; iter < iterations; iter++)
        {
            using var sourceA = new SourceCache<Person, string>(p => p.UniqueKey);
            using var sourceB = new SourceCache<Person, string>(p => p.UniqueKey);
            var errors = new ConcurrentQueue<Exception>();

            using var aToB = pipeline(sourceA.Connect().Filter(x => x.Name.StartsWith("A"))).ValidateSynchronization().RecordErrors(errors).PopulateInto(sourceB);
            using var bToA = pipeline(sourceB.Connect().Filter(x => x.Name.StartsWith("B"))).ValidateSynchronization().RecordErrors(errors).PopulateInto(sourceA);

            var participants = subjectPusher is null ? 2 : 3;
            using var barrier = new Barrier(participants);
            var taskA = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceA.AddOrUpdate(new Person("A-" + iter + "-" + i, i)); });
            var taskB = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceB.AddOrUpdate(new Person("B-" + iter + "-" + i, i)); });
            var taskC = subjectPusher is null ? null : RunOnDedicatedThread(() => { barrier.SignalAndWait(); subjectPusher(); });

            var completed = taskC is null ? Task.WhenAll(taskA, taskB) : Task.WhenAll(taskA, taskB, taskC);
            if (await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))) != completed)
                return false;

            await completed;
            await Assert.That(errors.IsEmpty).IsTrue().Because("iteration " + iter + ": " + string.Join(Environment.NewLine, errors));
            await Assert.That(sourceA.Items.Count(p => p.Name.StartsWith("A-" + iter + "-"))).IsEqualTo(ItemCount);
            await Assert.That(sourceB.Items.Count(p => p.Name.StartsWith("B-" + iter + "-"))).IsEqualTo(ItemCount);

            // Each destination must end up mirroring what a fresh subscription to the same pipeline
            // over the other source produces.
            await AssertPropagated(pipeline, sourceA, sourceB, "A", iter, propagation);
            await AssertPropagated(pipeline, sourceB, sourceA, "B", iter, propagation);

            await Assert.That(errors.IsEmpty).IsTrue().Because("iteration " + iter + ": " + string.Join(Environment.NewLine, errors));
        }
        return true;
    }

    private static async Task AssertPropagated(
        Func<IObservable<IChangeSet<Person, string>>, IObservable<IChangeSet<Person, string>>> pipeline,
        SourceCache<Person, string> from,
        SourceCache<Person, string> to,
        string prefix,
        int iter,
        Propagation propagation)
    {
        // Operators that hop to a scheduler can still be delivering on a pool thread after the writers
        // return, so wait for quiescence.
        var deadline = DateTime.UtcNow.AddSeconds(TimeoutSeconds);
        Dictionary<string, string> expected;
        Dictionary<string, string> actual;
        while (true)
        {
            expected = [];
            using (pipeline(from.Connect().Filter(x => x.Name.StartsWith(prefix))).Subscribe(changes =>
            {
                foreach (var change in changes)
                {
                    if (change.Reason == ChangeReason.Remove)
                        expected.Remove(change.Key);
                    else
                        expected[change.Key] = change.Current.Name;
                }
            }))
            {
            }

            var ownKeys = from.KeyValues.Where(kv => kv.Value.Name.StartsWith(prefix + "-")).Select(kv => kv.Key).ToHashSet();
            actual = to.KeyValues.Where(kv => ownKeys.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value.Name);
            if ((actual.Count == expected.Count && !actual.Except(expected).Any()) || DateTime.UtcNow > deadline)
                break;

            await Task.Delay(10);
        }

        await Assert.That(actual).IsEquivalentTo(expected).Because("items from " + prefix + " in iteration " + iter);
        if (propagation == Propagation.All)
            await Assert.That(actual.Count).IsEqualTo(ItemCount).Because("items from " + prefix + " in iteration " + iter);
    }

    private enum Propagation
    {
        /// <summary>Every item written to one source reaches the other.</summary>
        All,

        /// <summary>The destination mirrors the pipeline's current window over the other source.</summary>
        Window,
    }

    private static Task RunOnDedicatedThread(Action action) =>
        Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    [Test] public async Task Sort_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age))))).IsTrue();

    [Test] public async Task AutoRefresh_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.AutoRefresh(p => p.Age)))).IsTrue();

    [Test] public async Task GroupOn_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Group(p => p.Age % 3).MergeMany(g => g.Cache.Connect())))).IsTrue();

    [Test] public async Task GroupWithImmutableState_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.GroupWithImmutableState(p => p.Age % 3).TransformMany(g => g.Items, p => p.UniqueKey)))).IsTrue();

    [Test] public async Task GroupOnWithRegrouper_DoesNotDeadlock()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.Group(p => p.Age % 3, regrouper).MergeMany(g => g.Cache.Connect()),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) regrouper.OnNext(default(Unit)); }))).IsTrue();
    }

    [Test] public async Task GroupOnDynamicSelector_DoesNotDeadlock()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age % 3);
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.Group(selector, regrouper).MergeMany(g => g.Cache.Connect()),
            subjectPusher: () =>
            {
                for (var j = 0; j < ItemCount; j++)
                {
                    selector.OnNext((p, _) => p.Age % (2 + (j % 4)));
                    regrouper.OnNext(default(Unit));
                }
            }))).IsTrue();
    }

    [Test] public async Task TransformAsyncWithForce_DoesNotDeadlock()
    {
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.TransformAsync(p => Task.FromResult(new Person("T-" + p.Name, p.Age)), force),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) force.OnNext(static (_, _) => true); }))).IsTrue();
    }

    [Test] public async Task Page_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Page(req),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) req.OnNext(new PageRequest(1 + (j % 4), 25 + (j % 4) * 25)); },
            propagation: Propagation.Window))).IsTrue();
    }

    [Test] public async Task SortAndPage_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.SortAndPage(SortExpressionComparer<Person>.Ascending(p => p.Age), req),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) req.OnNext(new PageRequest(1 + (j % 4), 25 + (j % 4) * 25)); },
            propagation: Propagation.Window))).IsTrue();
    }

    [Test] public async Task Virtualise_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Virtualise(req),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) req.OnNext(new VirtualRequest(j * 5, 25 + (j % 4) * 25)); },
            propagation: Propagation.Window))).IsTrue();
    }

    [Test] public async Task SortAndVirtualize_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.SortAndVirtualize(SortExpressionComparer<Person>.Ascending(p => p.Age), req),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) req.OnNext(new VirtualRequest(j * 5, 25 + (j % 4) * 25)); },
            propagation: Propagation.Window))).IsTrue();
    }

    [Test] public async Task QueryWhenChanged_DoesNotDeadlock()
    {
        for (var iter = 0; iter < Iterations; iter++)
        {
            using var sourceA = new SourceCache<Person, string>(p => p.UniqueKey);
            using var sourceB = new SourceCache<Person, string>(p => p.UniqueKey);

            // QueryWhenChanged with an itemChangedTrigger exercises the Merge branch.
            // A side-channel write into the other cache closes the same ABBA cycle that
            // PopulateInto would close for changeset-shaped operators.
            using var aToB = sourceA.Connect()
                .Filter(p => p.Name.StartsWith("A"))
                .QueryWhenChanged(p => p.WhenPropertyChanged(x => x.Age))
                .Subscribe(_ => sourceB.AddOrUpdate(new Person("A-marker", 0)));
            using var bToA = sourceB.Connect()
                .Filter(p => p.Name.StartsWith("B"))
                .QueryWhenChanged(p => p.WhenPropertyChanged(x => x.Age))
                .Subscribe(_ => sourceA.AddOrUpdate(new Person("B-marker", 0)));

            using var barrier = new Barrier(2);
            var taskA = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceA.AddOrUpdate(new Person("A-" + iter + "-" + i, i)); });
            var taskB = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceB.AddOrUpdate(new Person("B-" + iter + "-" + i, i)); });

            var completed = Task.WhenAll(taskA, taskB);
            await Assert.That((await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))))).IsSameReferenceAs(completed).Because("iteration " + iter);
        }
    }

    [Test] public async Task TransformWithForce_DoesNotDeadlock()
    {
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.Transform((p, k) => new Person("T-" + p.Name, p.Age), force),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) force.OnNext(static (p, _) => true); }))).IsTrue();
    }

    [Test] public async Task BatchIf_DoesNotDeadlock()
    {
        using var pause = new ReactiveUI.Primitives.Signals.StateSignal<bool>(false);
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.BatchIf(pause, false, (TimeSpan?)null),
            subjectPusher: () => { for (var j = 0; j < ItemCount; j++) pause.OnNext(j % 2 == 0); }))).IsTrue();
    }

    [Test] public async Task DisposeMany_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.DisposeMany()))).IsTrue();

    [Test] public async Task OnItemRemoved_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.OnItemRemoved(_ => { })))).IsTrue();

    [Test] public async Task AllDangerous_Stacked_DoNotDeadlock()
    {
        using var pageReq = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 100));
        using var virtReq = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 100));
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        await Assert.That((await RunBidirectionalDeadlockTest(
            s => s.GroupWithImmutableState(p => p.Age % 3)
                  .TransformMany(g => g.Items, p => p.UniqueKey)
                  .AutoRefresh(p => p.Age)
                  .Filter(p => p.Age >= 0)
                  .Transform((p, k) => new Person("X-" + p.Name, p.Age), force)
                  .OnItemRemoved(_ => { })
                  .DisposeMany()
                  .Sort(SortExpressionComparer<Person>.Ascending(p => p.Age))
                  .Virtualise(virtReq)
                  .Page(pageReq),
            subjectPusher: () =>
            {
                for (var j = 0; j < ItemCount; j++)
                {
                    force.OnNext(static (p, _) => true);
                    pageReq.OnNext(new PageRequest(1 + (j % 4), 50 + (j % 4) * 50));
                    virtReq.OnNext(new VirtualRequest(j * 5, 50 + (j % 4) * 50));
                }
            },
            iterations: Iterations * 2,
            propagation: Propagation.Window))).IsTrue();
    }

    [Test] public async Task MultiplePairs_Simultaneous_NoDeadlock()
    {
        using var pageReq = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 50));
        using var pageReq2 = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 50));
        using var virtReq = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 50));
        using var virtReq2 = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 50));
        using var pause = new ReactiveUI.Primitives.Signals.StateSignal<bool>(false);
        var results = await Task.WhenAll(
            RunBidirectionalDeadlockTest(s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)), iterations: 30),
            RunBidirectionalDeadlockTest(s => s.AutoRefresh(p => p.Age), iterations: 30),
            RunBidirectionalDeadlockTest(s => s.Group(p => p.Age % 3).MergeMany(g => g.Cache.Connect()), iterations: 30),
            RunBidirectionalDeadlockTest(s => s.GroupWithImmutableState(p => p.Age % 3).TransformMany(g => g.Items, p => p.UniqueKey), iterations: 30),
            RunBidirectionalDeadlockTest(s => s.OnItemRemoved(_ => { }), iterations: 30),
            RunBidirectionalDeadlockTest(s => s.DisposeMany(), iterations: 30),
            RunBidirectionalDeadlockTest(
                s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Page(pageReq),
                subjectPusher: () => { for (var j = 0; j < ItemCount; j++) pageReq.OnNext(new PageRequest(1 + (j % 4), 25 + (j % 4) * 25)); },
                iterations: 30, propagation: Propagation.Window),
            RunBidirectionalDeadlockTest(
                s => s.SortAndPage(SortExpressionComparer<Person>.Ascending(p => p.Age), pageReq2),
                subjectPusher: () => { for (var j = 0; j < ItemCount; j++) pageReq2.OnNext(new PageRequest(1 + (j % 4), 25 + (j % 4) * 25)); },
                iterations: 30, propagation: Propagation.Window),
            RunBidirectionalDeadlockTest(
                s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Virtualise(virtReq),
                subjectPusher: () => { for (var j = 0; j < ItemCount; j++) virtReq.OnNext(new VirtualRequest(j * 5, 25 + (j % 4) * 25)); },
                iterations: 30, propagation: Propagation.Window),
            RunBidirectionalDeadlockTest(
                s => s.SortAndVirtualize(SortExpressionComparer<Person>.Ascending(p => p.Age), virtReq2),
                subjectPusher: () => { for (var j = 0; j < ItemCount; j++) virtReq2.OnNext(new VirtualRequest(j * 5, 25 + (j % 4) * 25)); },
                iterations: 30, propagation: Propagation.Window),
            RunBidirectionalDeadlockTest(
                s => s.BatchIf(pause, false, (TimeSpan?)null),
                subjectPusher: () => { for (var j = 0; j < ItemCount; j++) pause.OnNext(j % 2 == 0); },
                iterations: 30));
        await Assert.That(results.All(r => r)).IsTrue();
    }

    [Test] public async Task ThreeWayCircular_DoesNotDeadlock()
    {
        for (var iter = 0; iter < Iterations; iter++)
        {
            using var a = new SourceCache<Person, string>(p => p.UniqueKey);
            using var b = new SourceCache<Person, string>(p => p.UniqueKey);
            using var c = new SourceCache<Person, string>(p => p.UniqueKey);

            using var ab = a.Connect().Filter(p => p.Name.StartsWith("A")).Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).PopulateInto(b);
            using var bc = b.Connect().Filter(p => p.Name.StartsWith("A")).AutoRefresh(p => p.Age).PopulateInto(c);
            using var ca = c.Connect().Filter(p => p.Name.StartsWith("A")).Transform((p, _) => new Person("C-" + p.Name, p.Age)).Filter(p => p.Name.StartsWith("C")).PopulateInto(a);

            using var barrier = new Barrier(3);
            var tasks = new[]
            {
                RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) a.AddOrUpdate(new Person("A-" + iter + "-" + i, i)); }),
                RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) b.AddOrUpdate(new Person("B-" + iter + "-" + i, i)); }),
                RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) c.AddOrUpdate(new Person("CC-" + iter + "-" + i, i)); }),
            };
            var completed = Task.WhenAll(tasks);
            await Assert.That((await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))))).IsSameReferenceAs(completed).Because("iteration " + iter);
        }
    }

    [Test] public async Task TransformToTree_DoesNotDeadlock()
    {
        // Cross-cache cycle is closed via a side-channel Subscribe that writes a marker
        // into the other cache for every tree changeset.
        for (var iter = 0; iter < Iterations; iter++)
        {
            using var sourceA = new SourceCache<Person, string>(p => p.UniqueKey);
            using var sourceB = new SourceCache<Person, string>(p => p.UniqueKey);

            // The pivotOn function returns the parent's key (or the item's own key for roots). Half the
            // items become children of "A-{iter}-0" / "B-{iter}-0", populating the inner tree structure.
            using var aToB = sourceA.Connect()
                .TransformToTree(p => p.Age == 0 ? p.UniqueKey : "A-" + iter + "-0")
                .Subscribe(_ => sourceB.AddOrUpdate(new Person("from-a-tree-" + iter, 0)));
            using var bToA = sourceB.Connect()
                .TransformToTree(p => p.Age == 0 ? p.UniqueKey : "B-" + iter + "-0")
                .Subscribe(_ => sourceA.AddOrUpdate(new Person("from-b-tree-" + iter, 0)));

            using var barrier = new Barrier(2);
            var taskA = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceA.AddOrUpdate(new Person("A-" + iter + "-" + i, i)); });
            var taskB = RunOnDedicatedThread(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceB.AddOrUpdate(new Person("B-" + iter + "-" + i, i)); });

            var completed = Task.WhenAll(taskA, taskB);
            await Assert.That((await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))))).IsSameReferenceAs(completed).Because("iteration " + iter);
        }
    }

    [Test] public async Task Switch_DoesNotDeadlock() =>
        // Observable.Return(s).Switch() drives exactly one outer notification, which is enough
        // to wire up the destination cache and exercise its inner-change delivery path.
        await Assert.That((await RunBidirectionalDeadlockTest(s => Observable.Return(s).Switch()))).IsTrue();
}
