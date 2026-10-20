#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Internal;

/// <summary>
/// DD-044 (fork issue #37): a subscriber that throws at a value of the initial snapshot, delivered inside
/// <c>Subscribe</c>. The exception is thrown from <c>Subscribe</c>, is not delivered as <c>OnError</c>, and ends
/// the subscription: nothing else reaches the subscriber. Every pipeline catalogue of this fixture is covered.
/// </summary>
public partial class CallbackErrorDeliveryFixture
{
    // These pipelines deliver nothing while they are subscribed.
    private static readonly HashSet<string> PipelinesWithoutInitialValue = ["SkipInitial"];

    [Test]
    [MethodDataSource(nameof(CachePipelines))]
    public async Task CacheInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);

        await InitialSnapshotOutcome.Run(BuildCache(cache, pipeline), () => cache.AddOrUpdate(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(ListPipelines))]
    public async Task ListInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);

        await InitialSnapshotOutcome.Run(BuildList(list, pipeline), () => list.Add(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(CacheOperatorPipelines))]
    public async Task CacheOperatorInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var cache = new SourceCache<int, int>(static value => value);
        cache.AddOrUpdate(0);

        await InitialSnapshotOutcome.Run(BuildCacheOperator(cache, pipeline), () => cache.AddOrUpdate(1)).AssertThrownFromSubscribe(pipeline);
    }

    [Test]
    [MethodDataSource(nameof(ListOperatorPipelines))]
    public async Task ListOperatorInitialSnapshotSubscriberExceptionIsThrownFromSubscribe(string pipeline)
    {
        using var list = new SourceList<int>();
        list.Add(0);

        await InitialSnapshotOutcome.Run(BuildListOperator(list, pipeline), () => list.Add(1)).AssertThrownFromSubscribe(pipeline);
    }

    private sealed record InitialSnapshotOutcome(Exception Expected, Exception? Thrown, Exception? Error, int Deliveries, bool IsSubscriberReachable, Exception? LaterThrown)
    {
        public static InitialSnapshotOutcome Run(IObservable<object> source, Action laterEdit)
        {
            var expected = new InvalidOperationException("subscriber");
            var (thrown, subscriber, reference) = Subscribe(source, expected);

            // Anything that still holds the subscriber is reachable from the inputs, which the pipeline and the
            // caller keep alive.
            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            var isReachable = reference.IsAlive;
            var laterThrown = CaptureThrown(laterEdit);
            GC.KeepAlive(source);

            return new InitialSnapshotOutcome(expected, thrown, subscriber.Error, subscriber.Deliveries, isReachable, laterThrown);
        }

        public async Task AssertThrownFromSubscribe(string pipeline)
        {
            if (PipelinesWithoutInitialValue.Contains(pipeline))
            {
                await Assert.That(Deliveries).IsEqualTo(0);
                await Assert.That(Thrown).IsNull();
                return;
            }

            await Assert.That(Thrown).IsSameReferenceAs(Expected);
            await Assert.That(Error).IsNull();
            await Assert.That(Deliveries).IsEqualTo(1);
                await Assert.That(LaterThrown).IsNull();
        }

        // Not inlined, so that nothing but the subscription can keep the subscriber alive.
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static (Exception? Thrown, Counts Subscriber, WeakReference Reference) Subscribe(IObservable<object> source, Exception expected)
        {
            var counts = new Counts();
            var subscriber = new ThrowingSubscriber(expected, counts);
            var reference = new WeakReference(subscriber);
            Exception? thrown = null;
            try
            {
                // Disposed when Subscribe returns, which it only does for a pipeline without an initial value.
                source.Subscribe(subscriber).Dispose();
            }
            catch (Exception ex)
            {
                thrown = ex;
            }

            return (thrown, counts, reference);
        }
    }

    private sealed class Counts
    {
        public int Deliveries { get; set; }

        public Exception? Error { get; set; }
    }

    private sealed class ThrowingSubscriber(Exception expected, Counts counts) : IObserver<object>
    {
        public void OnNext(object value)
        {
            counts.Deliveries++;
            throw expected;
        }

        public void OnError(Exception error) => counts.Error = error;

        public void OnCompleted()
        {
        }
    }
}
