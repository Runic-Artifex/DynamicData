namespace DynamicData.Tests.Internal;

public sealed partial class UnsynchronizedCombineLatestFixture
{
    [Test]
    public async Task SelectorFailureIsReportedAsError()
    {
        using var first = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<int>();
        var failure = new InvalidOperationException("selector");
        Func<int, int, int> selector = (_, _) => throw failure;
        // The shared helper uses the Rx contract in both flavors. Primitives 9's ordinary
        // CombineLatest currently lets selector failures escape the producer.
#if REACTIVE_TESTS
        using var ordinary = first.CombineLatest(second, selector).RecordValues(out var expected);
#endif
        using var queued = first.UnsynchronizedCombineLatest(second, selector).RecordValues(out var actual);

        first.OnNext(1);
        second.OnNext(2);
        first.OnNext(3);
        first.OnCompleted();
        second.OnCompleted();

        await Assert.That(actual.Error).IsSameReferenceAs(failure);
        await Assert.That(actual.HasCompleted).IsFalse();
#if REACTIVE_TESTS
        await Assert.That(actual.Error).IsSameReferenceAs(expected.Error);
        await Assert.That(actual.HasCompleted).IsEqualTo(expected.HasCompleted);
#endif
        await Assert.That(actual.RecordedValues).IsEmpty();
        await Assert.That(actual.Notifications).HasCount(1);
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task EmptyCompletedSourceCompletesWhenOtherSourceEmits(bool completeFirst)
    {
        using var first = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<int>();
        // Ordinary Primitives 9 waits for both completions in this case. The shared
        // helper follows Rx's no-possible-pair completion rule in both flavors.
#if REACTIVE_TESTS
        using var ordinary = first.CombineLatest(second, (a, b) => a + b).RecordValues(out var expected);
#endif
        using var queued = first.UnsynchronizedCombineLatest(second, (a, b) => a + b).RecordValues(out var actual);
        var empty = completeFirst ? first : second;
        var live = completeFirst ? second : first;

        empty.OnCompleted();
        await Assert.That(actual.HasCompleted).IsFalse();
#if REACTIVE_TESTS
        await Assert.That(actual.HasCompleted).IsEqualTo(expected.HasCompleted);
#endif
        live.OnNext(1);
        await Assert.That(actual.HasCompleted).IsTrue();
#if REACTIVE_TESTS
        await Assert.That(actual.HasCompleted).IsEqualTo(expected.HasCompleted);
#endif
        live.OnCompleted();
        await Assert.That(actual.RecordedValues).IsEmpty();
        await Assert.That(actual.Error).IsNull();
    }

    [Test]
    public async Task NullValueMatchesCombineLatest()
    {
        using var first = new ReactiveUI.Primitives.Signals.Signal<string>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<int>();
        static string Select(string text, int number) => $"{text ?? "null"}:{number}";
        using var ordinary = first.CombineLatest(second, Select).RecordValues(out var expected);
        using var queued = first.UnsynchronizedCombineLatest(second, Select).RecordValues(out var actual);

        first.OnNext(null!);
        second.OnNext(2);
        first.OnNext("present");
        first.OnCompleted();
        second.OnNext(3);
        second.OnCompleted();

        await Assert.That(expected.RecordedValues).IsEquivalentTo(new[] { "null:2", "present:2", "present:3" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(actual.RecordedValues).IsEquivalentTo(expected.RecordedValues, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(actual.HasCompleted).IsEqualTo(expected.HasCompleted);
        await Assert.That(actual.Error).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SelectorFaultAndDisposalReleaseOnlySubscriptions(bool fault)
    {
        using var first = new OwnedSource<int>();
        using var second = new OwnedSource<int>();
        var failure = new OperationCanceledException("selector cancellation");
        using var subscription = first.UnsynchronizedCombineLatest(second, (a, b) => fault ? throw failure : a + b)
            .RecordValues(out var results);
        await Assert.That(first.ActiveSubscriptions).IsEqualTo(1);
        await Assert.That(second.ActiveSubscriptions).IsEqualTo(1);

        if (fault)
        {
            first.OnNext(1);
            second.OnNext(2);
            await Assert.That(results.Error).IsSameReferenceAs(failure);
            await Assert.That(results.RecordedValues).IsEmpty();
            await Assert.That(results.Notifications).HasCount(1);
        }
        else
        {
            subscription.Dispose();
        }

        await Assert.That(first.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(second.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(first.Disposals).IsEqualTo(0);
        await Assert.That(second.Disposals).IsEqualTo(0);
    }

    [Test]
    public async Task ReentrantErrorFromSelectorPreventsFollowingValue()
    {
        using var first = new ReactiveUI.Primitives.Signals.Signal<int>();
        using var second = new ReactiveUI.Primitives.Signals.Signal<int>();
        var failure = new InvalidOperationException("reentrant source failure");
        using var subscription = first.UnsynchronizedCombineLatest(second, (a, b) =>
        {
            first.OnError(failure);
            return a + b;
        }).RecordValues(out var results);

        first.OnNext(1);
        second.OnNext(2);
        second.OnNext(3);
        second.OnCompleted();

        await Assert.That(results.Error).IsSameReferenceAs(failure);
        await Assert.That(results.RecordedValues).IsEmpty();
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.Notifications).HasCount(1);
    }

    private sealed class OwnedSource<T> : IObservable<T>, IDisposable
    {
        private readonly ReactiveUI.Primitives.Signals.Signal<T> _source = new();

        public int ActiveSubscriptions { get; private set; }

        public int Disposals { get; private set; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            ActiveSubscriptions++;
            var subscription = _source.Subscribe(observer);
            return Disposable.Create(() =>
            {
                subscription.Dispose();
                ActiveSubscriptions--;
            });
        }

        public void OnNext(T value) => _source.OnNext(value);

        public void Dispose()
        {
            Disposals++;
            _source.Dispose();
        }
    }
}
