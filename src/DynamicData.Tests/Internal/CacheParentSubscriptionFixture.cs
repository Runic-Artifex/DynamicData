// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Bogus;

namespace DynamicData.Tests.Internal;

/// <summary>
/// Tests for <see cref="CacheParentSubscription{TParent, TKey, TChild, TObserver}"/>
/// behavioral contracts using a minimal concrete subclass.
/// </summary>
[NotInParallel]
public sealed class CacheParentSubscriptionFixture
{
    private const int SeedMin = 1;
    private const int SeedMax = 10000;
    private const int BatchSizeMin = 2;
    private const int BatchSizeMax = 8;

    private readonly Randomizer _rand = new(55);

    /// <summary>Test item with a typed key — no string parsing.</summary>
    private sealed record TestItem(int Key, string Value);

    [Test]
    public async Task ParentOnNext_CalledForEachChangeSet()
    {
        var itemCount = _rand.Number(BatchSizeMin, BatchSizeMax);
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer);
        sub.ExposeCreateParent(source.Connect());

        var items = Enumerable.Range(0, itemCount)
            .Select(i => new TestItem(_rand.Number(SeedMin, SeedMax) + i * 100, _rand.String2(_rand.Number(3, 10))))
            .ToList();

        foreach (var item in items)
            source.AddOrUpdate(item);

        await Assert.That(sub.ParentCallCount).IsEqualTo(items.Count);
        await Assert.That(observer.EmitCount).IsEqualTo(items.Count);
    }

    [Test]
    public async Task ChildOnNext_CalledForEachEmission()
    {
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var childSubjects = new List<ReactiveUI.Primitives.Signals.Signal<string>>();
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer, key =>
        {
            var subj = new ReactiveUI.Primitives.Signals.Signal<string>();
            childSubjects.Add(subj);
            return subj;
        });
        sub.ExposeCreateParent(source.Connect());

        var key = _rand.Number(SeedMin, SeedMax);
        source.AddOrUpdate(new TestItem(key, "parent"));

        await Assert.That(childSubjects).HasCount(1);
        var childValue = _rand.String2(_rand.Number(5, 15));
        childSubjects[0].OnNext(childValue);

        await Assert.That(sub.ChildCalls).HasCount(1);
        await Assert.That(sub.ChildCalls[0]).IsEqualTo((childValue, key));
    }

    [Test]
    public async Task EmitChanges_FiresOnceForBatch()
    {
        var batchSize = _rand.Number(BatchSizeMin, BatchSizeMax);
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer);
        sub.ExposeCreateParent(source.Connect());

        source.Edit(updater =>
        {
            for (var i = 0; i < batchSize; i++)
                updater.AddOrUpdate(new TestItem(i + 1, _rand.String2(_rand.Number(3, 8))));
        });

        await Assert.That(sub.ParentCallCount).IsEqualTo(1);
        await Assert.That(sub.EmitCallCount).IsEqualTo(1);
    }

    [Test]
    public async Task Batching_ChildUpdatesSettleBeforeEmit()
    {
        var batchSize = _rand.Number(BatchSizeMin, BatchSizeMax);
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var observer = new TestObserver();
        var childCount = 0;
        using var sub = new TestSubscription(observer, key =>
        {
            Interlocked.Increment(ref childCount);
            return new ReactiveUI.Primitives.Signals.StateSignal<string>($"sync-{key}");
        });
        sub.ExposeCreateParent(source.Connect());

        source.Edit(updater =>
        {
            for (var i = 0; i < batchSize; i++)
                updater.AddOrUpdate(new TestItem(i + 1, _rand.String2(_rand.Number(3, 8))));
        });

        await Assert.That(childCount).IsEqualTo(batchSize);
        await Assert.That(sub.EmitCallCount).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task Completion_RequiresParentAndAllChildren()
    {
        using var source = new TestSourceCache<TestItem, int>(x => x.Key);
        var childSubjects = new List<ReactiveUI.Primitives.Signals.Signal<string>>();
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer, key =>
        {
            var subj = new ReactiveUI.Primitives.Signals.Signal<string>();
            childSubjects.Add(subj);
            return subj;
        });
        sub.ExposeCreateParent(source.Connect());

        source.AddOrUpdate(new TestItem(_rand.Number(SeedMin, SeedMax), "item"));
        await Assert.That(childSubjects).HasCount(1);

        source.Complete();
        await Assert.That(observer.IsCompleted).IsFalse();

        childSubjects[0].OnCompleted();
        await Assert.That(observer.IsCompleted).IsTrue();
    }

    [Test]
    public async Task Completion_ParentOnly_NoChildren()
    {
        using var source = new TestSourceCache<TestItem, int>(x => x.Key);
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer);
        sub.ExposeCreateParent(source.Connect());

        source.Complete();
        await Assert.That(observer.IsCompleted).IsTrue();
    }

    [Test]
    public async Task Disposal_StopsAllEmissions()
    {
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var childSubjects = new List<ReactiveUI.Primitives.Signals.Signal<string>>();
        var observer = new TestObserver();
        var sub = new TestSubscription(observer, key =>
        {
            var subj = new ReactiveUI.Primitives.Signals.Signal<string>();
            childSubjects.Add(subj);
            return subj;
        });
        sub.ExposeCreateParent(source.Connect());

        source.AddOrUpdate(new TestItem(_rand.Number(SeedMin, SeedMax), "item"));
        var emitsBefore = observer.EmitCount;

        sub.Dispose();

        source.AddOrUpdate(new TestItem(_rand.Number(SeedMin + SeedMax, SeedMax * 2), "after"));
        if (childSubjects.Count > 0)
            childSubjects[0].OnNext("after-dispose");

        await Assert.That(observer.EmitCount).IsEqualTo(emitsBefore);
    }

    [Test]
    public async Task Error_Propagates()
    {
        using var source = new TestSourceCache<TestItem, int>(x => x.Key);
        var observer = new TestObserver();
        using var sub = new TestSubscription(observer);
        sub.ExposeCreateParent(source.Connect());

        var error = new InvalidOperationException("test error");
        source.SetError(error);

        await Assert.That(observer.Error).IsSameReferenceAs(error);
    }

    [Test]
    public async Task Serialization_ParentAndChildDoNotInterleave()
    {
        using var source = new SourceCache<TestItem, int>(x => x.Key);
        var callLog = new List<string>();
        var observer = new TestObserver();
        using var sub = new TestSubscription(
            observer,
            key =>
            {
                var subj = new ReactiveUI.Primitives.Signals.Signal<string>();
                return subj;
            },
            onParent: () => { lock (callLog) callLog.Add("P-start"); Thread.Sleep(1); lock (callLog) callLog.Add("P-end"); },
            onChild: () => { lock (callLog) callLog.Add("C-start"); Thread.Sleep(1); lock (callLog) callLog.Add("C-end"); });
        sub.ExposeCreateParent(source.Connect());

        source.AddOrUpdate(new TestItem(_rand.Number(SeedMin, SeedMax), "item"));

        // Start/end pairs should not interleave
        for (var i = 0; i + 1 < callLog.Count; i += 2)
        {
            var prefix = callLog[i].Split('-')[0];
            await Assert.That(callLog[i + 1]).StartsWith(prefix);
        }
    }

    /// <summary>
    /// Two subscriptions whose emissions feed each other, both through the other's child observable and through
    /// the other's source cache. Each writer thread is parked inside its own subscription's delivery until the
    /// other is too, and only then feeds the other, so the cycle is exercised on every run. If either the
    /// subscription or the cache held its lock while delivering, each writer would block on the lock the other
    /// holds. Ported from upstream #1213.
    /// </summary>
    [Test]
    [Timeout(10_000)]
    public async Task CrossFeedingSubscriptions_FeedEachOtherDuringDelivery_DoNotDeadlock(CancellationToken cancellationToken)
    {
        // Arrange
        var keys = _rand.Shuffle(Enumerable.Range(1, 1_000)).Take(4).ToArray();
        var (seedA, seedB) = (new TestItem(keys[0], _rand.String2(8)), new TestItem(keys[1], _rand.String2(8)));
        var (crossFromA, crossFromB) = (new TestItem(keys[2], _rand.String2(8)), new TestItem(keys[3], _rand.String2(8)));
        var (childValueFromA, childValueFromB) = (_rand.String2(8), _rand.String2(8));

        using var sourceA = new SourceCache<TestItem, int>(x => x.Key);
        using var sourceB = new SourceCache<TestItem, int>(x => x.Key);
        using var childOfA = new ReactiveUI.Primitives.Signals.Signal<string>();
        using var childOfB = new ReactiveUI.Primitives.Signals.Signal<string>();
        using var rendezvous = new Barrier(2);

        // The child is fed before the cache, so it only reaches the subscription made for the seed.
        var observerA = new CrossFeedObserver(rendezvous, () => { childOfB.OnNext(childValueFromA); sourceB.AddOrUpdate(crossFromA); });
        using var subA = new TestSubscription(observerA, _ => childOfA);
        subA.ExposeCreateParent(sourceA.Connect());

        var observerB = new CrossFeedObserver(rendezvous, () => { childOfA.OnNext(childValueFromB); sourceA.AddOrUpdate(crossFromB); });
        using var subB = new TestSubscription(observerB, _ => childOfB);
        subB.ExposeCreateParent(sourceB.Connect());

        // Act: the writers get threads of their own, so they never wait on a starved thread pool.
        var writerA = Task.Factory.StartNew(() => sourceA.AddOrUpdate(seedA), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var writerB = Task.Factory.StartNew(() => sourceB.AddOrUpdate(seedB), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await Task.WhenAll(writerA, writerB);

        // Assert
        await Assert.That(sourceA.Items).IsEquivalentTo(new[] { seedA, crossFromB });
        await Assert.That(sourceB.Items).IsEquivalentTo(new[] { seedB, crossFromA });
        await Assert.That(observerA.Received).IsEquivalentTo(new[] { seedA, seedA with { Value = childValueFromB }, crossFromB });
        await Assert.That(observerB.Received).IsEquivalentTo(new[] { seedB, seedB with { Value = childValueFromA }, crossFromA });
    }

    // ═══════════════════════════════════════════════════════════════
    // Test Infrastructure
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Records every item it receives. On its first emission it waits at <paramref name="rendezvous"/> for the
    /// other writer, then runs <paramref name="crossFeed"/>.
    /// </summary>
    private sealed class CrossFeedObserver(Barrier rendezvous, Action crossFeed) : IObserver<IChangeSet<TestItem, int>>
    {
        private bool _hasCrossFed;

        public List<TestItem> Received { get; } = [];

        public void OnNext(IChangeSet<TestItem, int> value)
        {
            Received.AddRange(value.Select(change => change.Current));

            if (_hasCrossFed)
                return;

            _hasCrossFed = true;
            rendezvous.SignalAndWait();
            crossFeed();
        }

        public void OnError(Exception error) { }

        public void OnCompleted() { }
    }

    /// <summary>
    /// Minimal concrete CacheParentSubscription for testing.
    /// </summary>
    private sealed class TestSubscription : CacheParentSubscription<TestItem, int, string, IChangeSet<TestItem, int>>
    {
        private readonly Func<int, IObservable<string>>? _childFactory;
        private readonly Action? _onParent;
        private readonly Action? _onChild;
        private readonly ChangeAwareCache<TestItem, int> _cache = new();

        public int ParentCallCount;
        public int EmitCallCount;
        public readonly List<(string Value, int Key)> ChildCalls = [];

        public TestSubscription(
            IObserver<IChangeSet<TestItem, int>> observer,
            Func<int, IObservable<string>>? childFactory = null,
            Action? onParent = null,
            Action? onChild = null)
            : base(observer)
        {
            _childFactory = childFactory;
            _onParent = onParent;
            _onChild = onChild;
        }

        public void ExposeCreateParent(IObservable<IChangeSet<TestItem, int>> source)
            => CreateParentSubscription(source);

        protected override void ParentOnNext(IChangeSet<TestItem, int> changes)
        {
            Interlocked.Increment(ref ParentCallCount);
            _onParent?.Invoke();
            _cache.Clone(changes);

            if (_childFactory is not null)
            {
                foreach (var change in (ChangeSet<TestItem, int>)changes)
                {
                    if (change.Reason is ChangeReason.Add or ChangeReason.Update)
                        AddChildSubscription(MakeChildObservable(_childFactory(change.Key)), change.Key);
                    else if (change.Reason is ChangeReason.Remove)
                        RemoveChildSubscription(change.Key);
                }
            }
        }

        protected override void ChildOnNext(string child, int parentKey)
        {
            _onChild?.Invoke();
            ChildCalls.Add((child, parentKey));
            _cache.AddOrUpdate(new TestItem(parentKey, child), parentKey);
        }

        protected override void EmitChanges(IObserver<IChangeSet<TestItem, int>> observer)
        {
            Interlocked.Increment(ref EmitCallCount);
            var changes = _cache.CaptureChanges();
            if (changes.Count > 0)
                observer.OnNext(changes);
        }
    }

    /// <summary>Observer that records emissions, completion, and errors.</summary>
    private sealed class TestObserver : IObserver<IChangeSet<TestItem, int>>
    {
        public int EmitCount;
        public bool IsCompleted;
        public Exception? Error;

        public void OnNext(IChangeSet<TestItem, int> value) => Interlocked.Increment(ref EmitCount);
        public void OnError(Exception error) => Error = error;
        public void OnCompleted() => IsCompleted = true;
    }
}
