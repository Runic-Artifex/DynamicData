#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Internal;

/// <summary>
/// DD-044: accumulator failures in Scan-based operators are delivered as OnError in both flavours,
/// including downstream of another Scan-based operator such as Transform. ReactiveUI.Primitives'
/// own Scan threw them back to the producer, where SourceList/SourceCache's SubscribeSafe turned
/// them into an OnError that an upstream, already self-disposed Scan then dropped.
/// </summary>
public class AccumulatorErrorDeliveryFixture
{
    [Test]
    public async Task ScanDeliversAccumulatorFailureAsOnErrorAndStops()
    {
        // In the Primitives flavour this binds to the fork's internal Scan (DD-044); in the
        // System.Reactive flavour it is Observable.Scan. Both must honour the Rx contract.
        using var source = new Signal<int>();
        var expected = new InvalidOperationException("accumulator");
        var calls = 0;
        var values = new List<int>();
        Exception? error = null;

        using var subscription = source
            .Scan(0, (sum, value) =>
            {
                calls++;
                return value == 2 ? throw expected : sum + value;
            })
            .Subscribe(values.Add, ex => error = ex);

        source.OnNext(1);
        source.OnNext(2);
        source.OnNext(3);

        await Assert.That(values).IsEquivalentTo(new[] { 1 });
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(calls).IsEqualTo(2);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task ScanAfterScanDeliversAccumulatorFailureFromSourceList()
    {
        using var list = new SourceList<int>();
        var expected = new InvalidOperationException("accumulator");
        Exception? error = null;

        using var subscription = list.Connect()
            .Scan(0, static (count, changes) => count + changes.Count)
            .Scan(0, (sum, count) => count == 2 ? throw expected : sum + count)
            .Subscribe(_ => { }, ex => error = ex);

        list.Add(1);
        list.Add(2);

        await Assert.That(error).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task ListTransformAfterTransformDeliversFactoryFailure()
    {
        using var list = new SourceList<int>();
        var expected = new InvalidOperationException("factory");
        using var results = list.Connect()
            .Transform(static value => value)
            .Transform(value => value == 2 ? throw expected : value.ToString())
            .AsAggregator();

        list.Add(1);
        list.Add(2);
        list.Add(3);

        await Assert.That(results.Exception).IsSameReferenceAs(expected);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "1" });
    }

    [Test]
    public async Task ListTransformDeliversFactoryFailureFromPlainSource()
    {
        using var source = new Signal<IChangeSet<int>>();
        var expected = new InvalidOperationException("factory");
        using var results = source
            .Transform<int, string>(_ => throw expected)
            .AsAggregator();

        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });

        await Assert.That(results.Exception).IsSameReferenceAs(expected);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task CacheTransformAfterTransformDeliversFactoryFailure()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var expected = new InvalidOperationException("factory");
        using var results = cache.Connect()
            .Transform(static value => value)
            .Transform(value => value == 2 ? throw expected : value.ToString())
            .AsAggregator();

        cache.AddOrUpdate(1);
        cache.AddOrUpdate(2);

        await Assert.That(results.Error).IsSameReferenceAs(expected);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "1" });
    }

    [Test]
    public async Task TransformWithInlineUpdateAfterTransformDeliversUpdateFailure()
    {
        using var cache = new SourceCache<Mutable, int>(static item => item.Id);
        var expected = new InvalidOperationException("update");
        using var results = cache.Connect()
            .Transform(static item => item)
            .TransformWithInlineUpdate(static item => new Mutable(item.Id, item.Value), (_, _) => throw expected)
            .AsAggregator();

        cache.AddOrUpdate(new Mutable(1, 1));
        cache.AddOrUpdate(new Mutable(1, 2));

        await Assert.That(results.Error).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task ListSumAfterTransformDeliversSelectorFailure()
    {
        using var list = new SourceList<int>();
        var expected = new InvalidOperationException("selector");
        var values = new List<int>();
        Exception? error = null;

        using var subscription = list.Connect()
            .Transform(static value => value)
            .Sum(value => value == 2 ? throw expected : value)
            .Subscribe(values.Add, ex => error = ex);

        list.Add(1);
        list.Add(2);

        await Assert.That(values).IsEquivalentTo(new[] { 1 });
        await Assert.That(error).IsSameReferenceAs(expected);
    }

    [Test]
    public async Task CacheMaximumAfterTransformDeliversSelectorFailure()
    {
        using var cache = new SourceCache<int, int>(static value => value);
        var expected = new InvalidOperationException("selector");
        var values = new List<int>();
        Exception? error = null;

        using var subscription = cache.Connect()
            .Transform(static value => value)
            .Maximum(value => value == 2 ? throw expected : value)
            .Subscribe(values.Add, ex => error = ex);

        cache.AddOrUpdate(1);
        cache.AddOrUpdate(2);

        await Assert.That(values).IsEquivalentTo(new[] { 1 });
        await Assert.That(error).IsSameReferenceAs(expected);
    }

#if !REACTIVE_TESTS
    [Test]
    public async Task ScanForwardsErrorAfterDownstreamFailureIsConvertedUpstream()
    {
        // An upstream operator that converts a downstream OnNext exception into OnError (as
        // ReactiveUI.Primitives' SubscribeSafe does) must find the fork's Scan still alive to
        // forward it. Primitives flavour only: System.Reactive's subscription safeguard detaches
        // the subscriber before the converted OnError arrives.
        using var source = new Signal<int>();
        var expected = new InvalidOperationException("observer");
        Exception? error = null;

        using var subscription = new ConvertingObservable<int>(source)
            .Scan(0, static (sum, value) => sum + value)
            .Subscribe(sum => { if (sum == 3) throw expected; }, ex => error = ex);

        source.OnNext(1);
        source.OnNext(2);

        await Assert.That(error).IsSameReferenceAs(expected);
    }
#endif

    [Test]
    public async Task ScanAfterSourceListPropagatesDownstreamFailureToEditor()
    {
        // DD-044 (#19): in both flavours a subscriber's exception reaches the editing caller.
        using var list = new SourceList<int>();
        var expected = new InvalidOperationException("observer");
        Exception? error = null;
        Exception? thrown = null;

        using var subscription = list.Connect()
            .Scan(0, static (count, changes) => count + changes.Count)
            .Subscribe(count => { if (count == 2) throw expected; }, ex => error = ex);

        list.Add(1);
        try
        {
            list.Add(2);
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        await Assert.That(thrown).IsSameReferenceAs(expected);
        await Assert.That(error).IsNull();
    }

    private sealed record Mutable(int Id, int Value);

#if !REACTIVE_TESTS
    private sealed class ConvertingObservable<T>(IObservable<T> source) : IObservable<T>
    {
        public IDisposable Subscribe(IObserver<T> observer) => source.Subscribe(new ConvertingObserver(observer));

        private sealed class ConvertingObserver(IObserver<T> observer) : IObserver<T>
        {
            public void OnNext(T value)
            {
                try
                {
                    observer.OnNext(value);
                }
                catch (Exception ex)
                {
                    observer.OnError(ex);
                }
            }

            public void OnError(Exception error) => observer.OnError(error);

            public void OnCompleted() => observer.OnCompleted();
        }
    }
#endif
}
