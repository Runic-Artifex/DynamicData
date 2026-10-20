using System.Collections.Concurrent;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests;
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.List;

/// <summary>
/// Deadlock torture test for list operators. Every operator that previously used
/// <c>Synchronize(locker)</c> (holding the lock during downstream delivery) is wired into a
/// bidirectional cross-list pipeline. Two threads write simultaneously, creating the ABBA
/// lock cycle:
///   Thread A: sourceA._locker -> operator lock -> PopulateInto -> sourceB._locker
///   Thread B: sourceB._locker -> operator lock -> PopulateInto -> sourceA._locker
///
/// On origin/main (Synchronize(lock)): deadlocks reliably within seconds.
/// On the PR branch (SynchronizeSafe queue-drain): no deadlock possible.
/// </summary>
[NotInParallel]
public sealed class DeadlockTortureTest
{
    private const int ItemCount = 200;
    private const int Iterations = 50;
    private const int TimeoutSeconds = 15;

    private static async Task<bool> RunBidirectionalDeadlockTest(
        Func<IObservable<IChangeSet<Person>>, IObservable<IChangeSet<Person>>> pipeline,
        int iterations = Iterations,
        bool propagatesEveryItem = true)
    {
        for (var iter = 0; iter < iterations; iter++)
        {
            using var sourceA = new SourceList<Person>();
            using var sourceB = new SourceList<Person>();
            var errors = new ConcurrentQueue<Exception>();

            // Filter prefixes prevent the feedback loop: items from A flowing into B keep their
            // "A" prefix, so the B-to-A filter rejects them. Likewise for the reverse direction.
            using var aToB = pipeline(sourceA.Connect().Filter(p => p.Name.StartsWith("A"))).ValidateSynchronization().RecordErrors(errors).PopulateInto(sourceB);
            using var bToA = pipeline(sourceB.Connect().Filter(p => p.Name.StartsWith("B"))).ValidateSynchronization().RecordErrors(errors).PopulateInto(sourceA);

            using var barrier = new Barrier(2);
            var taskA = Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceA.Add(new Person("A-" + iter + "-" + i, i)); });
            var taskB = Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) sourceB.Add(new Person("B-" + iter + "-" + i, i)); });

            var completed = Task.WhenAll(taskA, taskB);
            if (await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))) != completed)
                return false;

            await completed;
            await Assert.That(errors.IsEmpty).IsTrue().Because("iteration " + iter + ": " + string.Join(Environment.NewLine, errors));
            await Assert.That(sourceA.Items.Count(p => p.Name.StartsWith("A-"))).IsEqualTo(ItemCount);
            await Assert.That(sourceB.Items.Count(p => p.Name.StartsWith("B-"))).IsEqualTo(ItemCount);

            // Each destination must end up holding what a fresh subscription to the same pipeline
            // over the other source produces.
            await AssertPropagated(pipeline, sourceA, sourceB, "A", iter, propagatesEveryItem);
            await AssertPropagated(pipeline, sourceB, sourceA, "B", iter, propagatesEveryItem);

            await Assert.That(errors.IsEmpty).IsTrue().Because("iteration " + iter + ": " + string.Join(Environment.NewLine, errors));
        }
        return true;
    }

    private static async Task AssertPropagated(
        Func<IObservable<IChangeSet<Person>>, IObservable<IChangeSet<Person>>> pipeline,
        SourceList<Person> from,
        SourceList<Person> to,
        string prefix,
        int iter,
        bool propagatesEveryItem)
    {
        // Operators that hop to a scheduler (BufferIf observes its pause state on the task pool) can
        // still be delivering on a pool thread after the writers return, so wait for quiescence.
        var deadline = DateTime.UtcNow.AddSeconds(TimeoutSeconds);
        string[] expected;
        string[] actual;
        while (true)
        {
            using (var fresh = pipeline(from.Connect().Filter(p => p.Name.StartsWith(prefix))).AsObservableList())
            {
                expected = fresh.Items.Select(p => p.Name).Order().ToArray();
            }

            actual = to.Items.Where(p => p.Name.StartsWith(prefix + "-")).Select(p => p.Name).Order().ToArray();
            if (actual.SequenceEqual(expected) || DateTime.UtcNow > deadline)
                break;

            await Task.Delay(10);
        }

        await Assert.That(actual).IsEquivalentTo(expected).Because("items from " + prefix + " in iteration " + iter);
        if (propagatesEveryItem)
            await Assert.That(actual.Length).IsEqualTo(ItemCount).Because("items from " + prefix + " in iteration " + iter);
    }

    [Test]
    public async Task Sort_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age))))).IsTrue();

    [Test]
    public async Task AutoRefresh_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.AutoRefresh(p => p.Age)))).IsTrue();

    [Test]
    public async Task Page_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Page(req), propagatesEveryItem: false))).IsTrue();
    }

    [Test]
    public async Task Virtualise_DoesNotDeadlock()
    {
        using var req = new ReactiveUI.Primitives.Signals.StateSignal<IVirtualRequest>(new VirtualRequest(0, 50));
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).Virtualise(req), propagatesEveryItem: false))).IsTrue();
    }

    [Test]
    public async Task FilterDynamic_DoesNotDeadlock()
    {
        using var pred = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, bool>>(_ => true);
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.Filter(pred)))).IsTrue();
    }

    [Test]
    public async Task BufferIf_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.BufferIf(new ReactiveUI.Primitives.Signals.StateSignal<bool>(false), false, (TimeSpan?)null)))).IsTrue();

    [Test]
    public async Task DisposeMany_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.DisposeMany()))).IsTrue();

    [Test]
    public async Task GroupOn_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.GroupOn(p => p.Age % 3).MergeMany(g => g.List.Connect())))).IsTrue();

    [Test]
    public async Task TransformMany_DoesNotDeadlock() =>
        await Assert.That((await RunBidirectionalDeadlockTest(s => s.TransformMany(p => new[] { p })))).IsTrue();

    [Test]
    public async Task AllDangerous_Stacked_DoNotDeadlock()
    {
        using var pageReq = new ReactiveUI.Primitives.Signals.StateSignal<IPageRequest>(new PageRequest(1, 100));
        using var pred = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, bool>>(_ => true);
        await Assert.That(await RunBidirectionalDeadlockTest(
            s => s.AutoRefresh(p => p.Age)
                  .Filter(pred)
                  .DisposeMany()
                  .Sort(SortExpressionComparer<Person>.Ascending(p => p.Age))
                  .Page(pageReq),
            iterations: Iterations * 2,
            propagatesEveryItem: false)).IsTrue();
    }

    [Test]
    public async Task ThreeWayCircular_DoesNotDeadlock()
    {
        for (var iter = 0; iter < Iterations; iter++)
        {
            using var a = new SourceList<Person>();
            using var b = new SourceList<Person>();
            using var c = new SourceList<Person>();

            using var ab = a.Connect().Filter(p => p.Name.StartsWith("A")).Sort(SortExpressionComparer<Person>.Ascending(p => p.Age)).PopulateInto(b);
            using var bc = b.Connect().Filter(p => p.Name.StartsWith("A")).AutoRefresh(p => p.Age).PopulateInto(c);
            using var ca = c.Connect().Filter(p => p.Name.StartsWith("A")).Transform(p => new Person("C-" + p.Name, p.Age)).Filter(p => p.Name.StartsWith("C")).PopulateInto(a);

            using var barrier = new Barrier(3);
            var tasks = new[]
            {
                Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) a.Add(new Person("A-" + iter + "-" + i, i)); }),
                Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) b.Add(new Person("B-" + iter + "-" + i, i)); }),
                Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < ItemCount; i++) c.Add(new Person("CC-" + iter + "-" + i, i)); }),
            };
            var completed = Task.WhenAll(tasks);
            await Assert.That((await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds))))).IsSameReferenceAs(completed).Because("iteration " + iter);
        }
    }
}
