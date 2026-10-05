// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DynamicData.Tests.Internal;

/// <summary>
/// Focused behavioural tests for <see cref="DeliveryQueueMergeExtensions.DeliveryQueueMerge{T}(IObservable{T}, IObservable{T}[])"/>.
/// Verifies the Rx Merge-compatible terminal semantics and the queue's serialization guarantee
/// for concurrent producers.
/// </summary>
public sealed class DeliveryQueueMergeFixture
{
    [Test]
    public async Task OnNext_FromAllSources_IsForwardedInArrivalOrder()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var c = new ReactiveUI.Primitives.Signals.Signal<int>();

        var received = new List<int>();
        using var sub = a.DeliveryQueueMerge(b, c).Subscribe(received.Add);

        a.OnNext(1);
        b.OnNext(2);
        c.OnNext(3);
        a.OnNext(4);

        await Assert.That(received).IsEquivalentTo(new[] { 1, 2, 3, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OnCompleted_FiresOnlyAfterEverySourceCompletes()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var c = new ReactiveUI.Primitives.Signals.Signal<int>();

        var completed = false;
        using var sub = a.DeliveryQueueMerge(b, c).Subscribe(_ => { }, () => completed = true);

        a.OnCompleted();
        await Assert.That(completed).IsFalse();

        b.OnCompleted();
        await Assert.That(completed).IsFalse();

        c.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task OnError_FromAnySource_TerminatesImmediately()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.DeliveryQueueMerge(b).Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException();
        a.OnError(error);

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    public async Task OnError_AfterFirstError_IsDroppedByQueue()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        using var sub = a.DeliveryQueueMerge(b).Subscribe(_ => { }, e => captured = e, () => { });

        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");
        a.OnError(first);
        b.OnError(second);

        await Assert.That(captured).IsSameReferenceAs(first);
    }

    [Test]
    public async Task OnCompleted_AfterError_IsDroppedByQueue()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.DeliveryQueueMerge(b).Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException();
        a.OnError(error);
        b.OnCompleted();

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    public async Task SynchronousTerminal_AtSubscribe_IsCountedTowardCompletion()
    {
        var immediate = Observable.Empty<int>();
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        var completed = false;
        using var sub = immediate.DeliveryQueueMerge(live).Subscribe(_ => { }, () => completed = true);

        await Assert.That(completed).IsFalse();
        live.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task SynchronousError_AtSubscribe_PropagatesImmediately()
    {
        var error = new InvalidOperationException();
        var immediate = Observable.Throw<int>(error);
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        using var sub = immediate.DeliveryQueueMerge(live).Subscribe(_ => { }, e => captured = e);

        await Assert.That(captured).IsSameReferenceAs(error);
    }

    [Test]
    public async Task ConcurrentOnNext_FromManyProducers_IsSerializedToObserver()
    {
        // The queue's contract is that the downstream observer never sees concurrent OnNext calls,
        // regardless of how many producers are racing on the inputs. Subscribe to two sources via
        // two concurrent tasks, push interleaved items, and verify that no two OnNext calls overlap
        // and every item is delivered exactly once.
        const int itemsPerProducer = 1_000;

        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        var inFlight = 0;
        var maxInFlight = 0;
        var received = new ConcurrentQueue<int>();

        using var sub = a.DeliveryQueueMerge(b).Subscribe(v =>
        {
            var now = Interlocked.Increment(ref inFlight);
            var prev = Volatile.Read(ref maxInFlight);
            while (now > prev && Interlocked.CompareExchange(ref maxInFlight, now, prev) != prev)
            {
                prev = Volatile.Read(ref maxInFlight);
            }
            received.Enqueue(v);
            Interlocked.Decrement(ref inFlight);
        });

        using var barrier = new Barrier(2);
        var taskA = Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < itemsPerProducer; i++) a.OnNext(i); });
        var taskB = Task.Run(() => { barrier.SignalAndWait(); for (var i = 0; i < itemsPerProducer; i++) b.OnNext(itemsPerProducer + i); });

        await Task.WhenAll(taskA, taskB);

        await Assert.That(received.Count).IsEqualTo(itemsPerProducer * 2);
        await Assert.That(maxInFlight).IsEqualTo(1).Because("concurrent OnNext to the observer must be serialized by the queue");

        var expected = Enumerable.Range(0, itemsPerProducer * 2).ToHashSet();
        await Assert.That(received).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Any);
    }

    [Test]
    public async Task Subscription_OccursInArgumentOrder()
    {
        var subscribed = new List<int>();
        var first = Observable.Create<int>(o => { subscribed.Add(0); return Disposable.Empty; });
        var second = Observable.Create<int>(o => { subscribed.Add(1); return Disposable.Empty; });
        var third = Observable.Create<int>(o => { subscribed.Add(2); return Disposable.Empty; });

        using var sub = first.DeliveryQueueMerge(second, third).Subscribe(_ => { });

        await Assert.That(subscribed).IsEquivalentTo(new[] { 0, 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task Dispose_StopsForwardingFromAnySource()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        var received = new List<int>();
        var sub = a.DeliveryQueueMerge(b).Subscribe(received.Add);

        a.OnNext(1);
        sub.Dispose();
        a.OnNext(2);
        b.OnNext(3);

        await Assert.That(received).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task NoOthers_FallsBackToFirstAlone()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        var received = new List<int>();
        var completed = false;
        using var sub = a.DeliveryQueueMerge().Subscribe(received.Add, () => completed = true);

        a.OnNext(7);
        a.OnNext(11);
        a.OnCompleted();

        await Assert.That(received).IsEquivalentTo(new[] { 7, 11 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(completed).IsTrue();
    }
}
