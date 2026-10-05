using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DynamicData.Tests.Cache;

public static partial class SourceCacheFixture
{
    public sealed class IntegrationTests
        : IntegrationTestFixtureBase
    {
        [Test]
        public async Task ConnectDuringDeliveryDoesNotDuplicate()
        {
            using var cache = new SourceCache<TestItem, string>(static item => item.Key);
            var delivering = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var connectDone = new ManualResetEventSlim(false);
            var firstDelivery = true;
            using var slowSubscription = cache.Connect().Subscribe(_ =>
            {
                if (!firstDelivery)
                    return;

                firstDelivery = false;
                delivering.TrySetResult();
                if (!connectDone.Wait(TimeSpan.FromSeconds(30)))
                    throw new TimeoutException("The second subscriber did not connect during delivery.");
            });

            // This writer intentionally blocks inside OnNext. Give it its own thread
            // so it cannot starve the pool that runs the test's async continuations.
            var firstWrite = Task.Factory.StartNew(
                () => cache.AddOrUpdate(new TestItem("k1", "v1")),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            try
            {
                await delivering.Task.WaitAsync(TimeSpan.FromSeconds(15));
                await Task.Run(() => cache.AddOrUpdate(new TestItem("k2", "v2")))
                    .WaitAsync(TimeSpan.FromSeconds(15));

                // Both writes are committed, but the second delivery is still queued.
                var addCounts = new Dictionary<string, int>();
                using var newSubscription = cache.Connect().Subscribe(changes =>
                {
                    foreach (var change in changes)
                    {
                        if (change.Reason == ChangeReason.Add)
                            addCounts[change.Key] = addCounts.GetValueOrDefault(change.Key) + 1;
                    }
                });

                connectDone.Set();
                await firstWrite.WaitAsync(TimeSpan.FromSeconds(15));
                await Assert.That(addCounts.GetValueOrDefault("k1")).IsEqualTo(1);
                await Assert.That(addCounts.GetValueOrDefault("k2")).IsEqualTo(1)
                    .Because("the queued update must not duplicate the subscription snapshot");
            }
            finally
            {
                connectDone.Set();
                await firstWrite.WaitAsync(TimeSpan.FromSeconds(15));
            }
        }
        [Test]
        public async Task DirectCrossWriteDoesNotDeadlock()
        {
            const int iterations = 50;

            for (var iter = 0; iter < iterations; iter++)
            {
                using var cacheA = new SourceCache<TestItem, string>(static x => x.Key);
                using var cacheB = new SourceCache<TestItem, string>(static x => x.Key);

                // Bidirectional: A items flow into B, B items flow into A.
                // Filter by prefix prevents infinite feedback.
                using var aToB = cacheA.Connect()
                    .Filter(static x => x.Key.StartsWith('a'))
                    .Transform(static (item, _) => new TestItem("from-a-" + item.Key, item.Value))
                    .PopulateInto(cacheB);

                using var bToA = cacheB.Connect()
                    .Filter(static x => x.Key.StartsWith('b'))
                    .Transform(static (item, _) => new TestItem("from-b-" + item.Key, item.Value))
                    .PopulateInto(cacheA);

                using var barrier = new Barrier(2);

                var taskA = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    for (var i = 0; i < 1000; i++)
                    {
                        cacheA.AddOrUpdate(new TestItem("a" + i, "V" + i));
                    }
                });

                var taskB = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    for (var i = 0; i < 1000; i++)
                    {
                        cacheB.AddOrUpdate(new TestItem("b" + i, "V" + i));
                    }
                });

                var completed = Task.WhenAll(taskA, taskB);
                var finished = await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(60)));

                await Assert.That(finished).IsSameReferenceAs(completed).Because($"iteration {iter}: bidirectional cross-cache writes should not deadlock");
            }
        }

        [Test]
        public async Task MultiCacheFanInDoesNotDeadlock()
        {
            const int itemCount = 100;

            using var cacheA = new SourceCache<TestItem, string>(static x => x.Key);
            using var cacheB = new SourceCache<TestItem, string>(static x => x.Key);
            using var destination = new SourceCache<TestItem, string>(static x => x.Key);
            using var subA = cacheA.Connect().PopulateInto(destination);
            using var subB = cacheB.Connect().PopulateInto(destination);
            using var results = destination.Connect().AsAggregator();

            var taskA = Task.Run(() =>
            {
                for (var i = 0; i < itemCount; i++)
                {
                    cacheA.AddOrUpdate(new TestItem($"a-{i}", $"ValueA-{i}"));
                }
            });

            var taskB = Task.Run(() =>
            {
                for (var i = 0; i < itemCount; i++)
                {
                    cacheB.AddOrUpdate(new TestItem($"b-{i}", $"ValueB-{i}"));
                }
            });

            var completed = Task.WhenAll(taskA, taskB);
            var finished = await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(10)));

            await Assert.That(finished).IsSameReferenceAs(completed).Because("concurrent edits with cross-cache subscribers should not deadlock");
            await Assert.That(results.Error).IsNull();
            await Assert.That(results.Data.Count).IsEqualTo(itemCount * 2).Because("all items from both caches should arrive in the destination");
            await Assert.That(results.Data.Items).IsEquivalentTo([.. cacheA.Items, .. cacheB.Items], TUnit.Assertions.Enums.CollectionOrdering.Any).Because("all items should be in the destination");
        }
    }
}
