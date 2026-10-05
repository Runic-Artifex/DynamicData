// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

namespace DynamicData.Tests.Internal;

/// <summary>
/// Focused behavioural tests for <see cref="SynchronizeSafeExtensions.UnsynchronizedCombineLatest{TFirst, TSecond, TResult}(IObservable{TFirst}, IObservable{TSecond}, Func{TFirst, TSecond, TResult})"/>.
/// Covers the contract the helper has to honour as a drop-in <see cref="Observable.CombineLatest{TFirst, TSecond, TResult}(IObservable{TFirst}, IObservable{TSecond}, Func{TFirst, TSecond, TResult})"/>
/// replacement: emits only after both sources have produced at least one value, then on every subsequent OnNext from either side; first error terminates;
/// completes only after both sources complete.
/// </summary>
public sealed class UnsynchronizedCombineLatestFixture
{
    [Test]
    public async Task OnNext_DoesNotEmit_UntilBothSourcesHaveProduced()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        var received = new List<string>();
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(received.Add);

        a.OnNext(1);
        await Assert.That(received).IsEmpty().Because("only the first source has produced");

        a.OnNext(2);
        await Assert.That(received).IsEmpty().Because("the second source still has not produced");

        b.OnNext("first");
        await Assert.That(received).IsEquivalentTo(new[] { "2:first" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OnNext_AfterBothHaveProduced_EmitsOnEverySubsequentValue()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        var received = new List<string>();
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(received.Add);

        a.OnNext(1);
        b.OnNext("x");
        a.OnNext(2);
        b.OnNext("y");
        b.OnNext("z");
        a.OnNext(3);

        await Assert.That(received).IsEquivalentTo(new[] { "1:x", "2:x", "2:y", "2:z", "3:z" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task OnCompleted_FiresOnlyAfterBothSourcesComplete()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        var completed = false;
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(_ => { }, () => completed = true);

        a.OnCompleted();
        await Assert.That(completed).IsFalse().Because("the second source is still live");

        b.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task OnError_FromAnySource_TerminatesImmediately()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException("first");
        b.OnError(error);

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse().Because("OnCompleted must not fire after OnError");
    }

    [Test]
    public async Task OnError_AfterFirstError_IsIgnored()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        Exception? captured = null;
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(_ => { }, e => captured = e, () => { });

        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");
        a.OnError(first);
        b.OnError(second);

        await Assert.That(captured).IsSameReferenceAs(first).Because("first error wins; subsequent errors from other sources must be dropped");
    }

    [Test]
    public async Task OnNext_AfterError_IsIgnored()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        var received = new List<string>();
        Exception? captured = null;
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(received.Add, e => captured = e);

        a.OnNext(1);
        b.OnNext("x");
        await Assert.That(received).IsEquivalentTo(new[] { "1:x" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        var error = new InvalidOperationException();
        a.OnError(error);

        a.OnNext(2);
        b.OnNext("y");
        await Assert.That(received).IsEquivalentTo(new[] { "1:x" }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("no further OnNext must arrive after OnError has fired");
        await Assert.That(captured).IsSameReferenceAs(error);
    }

    [Test]
    public async Task OnCompleted_AfterError_IsIgnored()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<string>();

        Exception? captured = null;
        var completed = false;
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => $"{x}:{y}").Subscribe(_ => { }, e => captured = e, () => completed = true);

        var error = new InvalidOperationException();
        a.OnError(error);
        b.OnCompleted();

        await Assert.That(captured).IsSameReferenceAs(error);
        await Assert.That(completed).IsFalse().Because("a late OnCompleted from a surviving source must not arrive after OnError");
    }

    [Test]
    public async Task ResultSelector_ReceivesMostRecentValueFromEachSource()
    {
        using var a = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var b = new ReactiveUI.Primitives.Signals.Signal<int>();

        var received = new List<int>();
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => x * 10 + y).Subscribe(received.Add);

        a.OnNext(1);
        b.OnNext(2);
        await Assert.That(received).IsEquivalentTo(new[] { 12 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        a.OnNext(3);
        await Assert.That(received).IsEquivalentTo(new[] { 12, 32 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the second source's most recent value (2) must still be in effect");

        b.OnNext(4);
        await Assert.That(received).IsEquivalentTo(new[] { 12, 32, 34 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task SynchronousValues_AtSubscribeTime_AreCombinedCorrectly()
    {
        // Behaviour subjects deliver their initial value synchronously at Subscribe time.
        // The helper must capture the first source's value before subscribing to the second,
        // and immediately emit when the second source's initial value arrives.
        using var a = new ReactiveUI.Primitives.Signals.StateSignal<int>(7);
        using var b = new ReactiveUI.Primitives.Signals.StateSignal<int>(11);

        var received = new List<int>();
        using var sub = a.UnsynchronizedCombineLatest(b, (x, y) => x + y).Subscribe(received.Add);

        await Assert.That(received).IsEquivalentTo(new[] { 18 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("both subjects delivered synchronously at subscribe time");
    }

    [Test]
    public async Task SynchronousCompletion_BeforeOther_StillCompletesOnlyAfterBoth()
    {
        var immediate = Observable.Empty<int>();
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        var completed = false;
        using var sub = immediate.UnsynchronizedCombineLatest(live, (x, y) => x + y).Subscribe(_ => { }, () => completed = true);

        await Assert.That(completed).IsFalse().Because("the live source has not completed yet");

        live.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    [Test]
    public async Task SynchronousError_BeforeOther_TerminatesImmediately()
    {
        var error = new InvalidOperationException();
        var immediate = Observable.Throw<int>(error);
        using var live = new ReactiveUI.Primitives.Signals.Signal<int>();

        Exception? captured = null;
        using var sub = immediate.UnsynchronizedCombineLatest(live, (x, y) => x + y).Subscribe(_ => { }, e => captured = e);

        await Assert.That(captured).IsSameReferenceAs(error);
    }
}
