#if REACTIVE_TESTS
using QueueInternals = DynamicData.Reactive.Internal;
#else
using QueueInternals = DynamicData.Internal;
#endif

namespace DynamicData.Tests.Internal;

public sealed class QueueNullNotificationsFixture
{
    [Test]
    public async Task NullAndDefaultPayloadsAreNextNotifications()
    {
        var reference = QueueInternals.Notification<string?>.CreateNext(null);
        var nullable = QueueInternals.Notification<int?>.CreateNext(null);
        var value = QueueInternals.Notification<int>.CreateNext(0);
        var references = new List<string?>();
        var nullables = new List<int?>();
        var values = new List<int>();
        var completions = 0;
        reference.Accept(Observer.Create<string?>(references.Add, () => completions++));
        nullable.Accept(Observer.Create<int?>(nullables.Add, () => completions++));
        value.Accept(Observer.Create<int>(values.Add, () => completions++));

        await Assert.That(reference.IsTerminal).IsFalse();
        await Assert.That(reference.IsError).IsFalse();
        await Assert.That(nullable.IsTerminal).IsFalse();
        await Assert.That(value.IsTerminal).IsFalse();
        await Assert.That(references).IsEquivalentTo(new string?[] { null }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(nullables).IsEquivalentTo(new int?[] { null }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(values).IsEquivalentTo(new[] { 0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(completions).IsEqualTo(0);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ScopedNullNextPreservesValuesAndTerminalKind(bool shared, bool fail)
    {
        var values = new List<string?>();
        Exception? observedError = null;
        var completions = 0;
        var error = new InvalidOperationException("terminal error");
        var observer = Observer.Create<string?>(values.Add, e => observedError = e, () => completions++);
        if (shared)
        {
            using var queue = new SharedDeliveryQueue();
            using var input = queue.CreateQueue(observer);
            using var scope = input.AcquireLock();
            scope.EnqueueNext(null);
            scope.EnqueueNext("after null");
            if (fail)
                scope.EnqueueError(error);
            else
                scope.EnqueueCompleted();
        }
        else
        {
            using var queue = new DeliveryQueue<string?>(observer);
            using var scope = queue.AcquireLock();
            scope.EnqueueNext(null);
            scope.EnqueueNext("after null");
            if (fail)
                scope.EnqueueError(error);
            else
                scope.EnqueueCompleted();
        }

        await Assert.That(values).IsEquivalentTo(new string?[] { null, "after null" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(observedError).IsSameReferenceAs(fail ? error : null);
        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
    }

    [Test]
    public async Task QueuedCombineLatestPreservesNullInputsAndResults()
    {
        using var queue = new SharedDeliveryQueue();
        using var first = new ReactiveUI.Primitives.Signals.Signal<string>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var subscription = first.SynchronizeSafe(queue)
            .UnsynchronizedCombineLatest(second.SynchronizeSafe(queue), (text, _) => text!)
            .SynchronizeSafe(queue)
            .RecordValues(out var results);

        first.OnNext(null!);
        second.OnNext(1);
        await Assert.That(results.HasCompleted).IsFalse();
        first.OnNext("after null");
        second.OnNext(2);
        first.OnCompleted();
        second.OnCompleted();

        await Assert.That(results.RecordedValues).IsEquivalentTo(new string?[] { null, "after null", "after null" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Notifications).HasCount(4);
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Error).IsNull();
    }
}
