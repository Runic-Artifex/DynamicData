// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

namespace DynamicData.Tests.Internal;

/// <summary>
/// Focused behavioural tests for <see cref="SynchronizeSafeExtensions.UnsynchronizedMerge{T}(IObservable{T}, IObservable{T}[])"/>.
/// Covers the contract the helper has to honour as a drop-in <see cref="Observable.Merge{TSource}(IObservable{TSource}[])"/>
/// replacement: subscription order, all-must-complete OnCompleted, first-error-wins OnError, and synchronous terminal
/// notifications.
/// </summary>
public sealed class UnsynchronizedMergeFixture
{
    [Test]
    public async Task OnNext_FromBothSources_IsForwardedInArrivalOrder()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        var received = new List<int>();
        using var sub = a.UnsynchronizedMerge(b).Subscribe(received.Add);

        a.OnNext(1);
        b.OnNext(2);
        a.OnNext(3);
        b.OnNext(4);

        await Assert.That(received).IsEquivalentTo(new[] { 1, 2, 3, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OnCompleted_FiresOnlyAfterAllSourcesComplete()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var c = new ReactiveUI.Primitives.Signals.Signal<int>();

        var completed = false;
        using var sub = a.UnsynchronizedMerge(b, c).Subscribe(_ => { }, () => completed = true);

        a.OnCompleted();
        await Assert.That(completed).IsFalse().Because("a single source completion must not terminate the merged stream");

        b.OnCompleted();
        await Assert.That(completed).IsFalse().Because("two of three completions still leave one source live");

        c.OnCompleted();
        await Assert.That(completed).IsTrue().Because("after every source has completed the merged stream must emit OnCompleted");
    }

    [Test]
    public async Task OnError_FromAnySource_TerminatesImmediately()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var c = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.UnsynchronizedMerge(b, c).Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException("first");
        b.OnError(error);

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse().Because("OnCompleted must not fire after OnError");
    }

    [Test]
    public async Task OnError_AfterFirstError_IsIgnored()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        using var sub = a.UnsynchronizedMerge(b).Subscribe(_ => { }, e => captured = e, () => { });

        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");
        a.OnError(first);
        b.OnError(second);

        await Assert.That(captured).IsSameReferenceAs(first).Because("first error wins; subsequent errors from other sources must be dropped");
    }

    [Test]
    public async Task OnCompleted_AfterError_IsIgnored()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.UnsynchronizedMerge(b).Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException();
        a.OnError(error);
        b.OnCompleted();

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse().Because("a late OnCompleted from a surviving source must not arrive after OnError has fired");
    }

    [Test]
    public async Task Subscription_OccursInArgumentOrder()
    {
        var subscribed = new List<int>();
        var first = Observable.Create<int>(o => { subscribed.Add(0); return Disposable.Empty; });
        var second = Observable.Create<int>(o => { subscribed.Add(1); return Disposable.Empty; });
        var third = Observable.Create<int>(o => { subscribed.Add(2); return Disposable.Empty; });

        using var sub = first.UnsynchronizedMerge(second, third).Subscribe(_ => { });

        await Assert.That(subscribed).IsEquivalentTo(new[] { 0, 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task SynchronousTerminal_BeforeOtherSourcesSubscribe_IsHandled()
    {
        // A source that completes synchronously at subscribe time decrements the pending counter immediately.
        // If the helper miscounted, the merged stream would either complete prematurely or never complete.
        var immediate = Observable.Empty<int>();
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        var completed = false;
        using var sub = immediate.UnsynchronizedMerge(live).Subscribe(_ => { }, () => completed = true);

        await Assert.That(completed).IsFalse().Because("the live source has not completed yet");

        live.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task SynchronousError_BeforeOtherSourcesSubscribe_TerminatesImmediately()
    {
        var error = new InvalidOperationException();
        var immediate = Observable.Throw<int>(error);
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        using var sub = immediate.UnsynchronizedMerge(live).Subscribe(_ => { }, e => captured = e);

        await Assert.That(captured).IsSameReferenceAs(error);
    }

    [Test]
    public async Task NoOthers_FallsBackToFirstAlone()
    {
        // Boundary: zero entries in the params array. Behaviour must mirror Observable.Merge over a single source.
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        var received = new List<int>();
        var completed = false;
        using var sub = a.UnsynchronizedMerge().Subscribe(received.Add, () => completed = true);

        a.OnNext(7);
        a.OnNext(11);
        a.OnCompleted();

        await Assert.That(received).IsEquivalentTo(new[] { 7, 11 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(completed).IsTrue();
    }
}
