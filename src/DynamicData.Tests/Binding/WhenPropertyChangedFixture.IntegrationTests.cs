using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DynamicData.Tests.Binding;

public static partial class WhenPropertyChangedFixture
{
    public sealed partial class IntegrationTests
        : IntegrationTestFixtureBase
    {
        private static readonly TimeSpan ConditionTimeout
            = TimeSpan.FromSeconds(30);

        private static void WaitForCondition(Func<bool> condition, TimeSpan? timeout = null) =>
            SpinWait.SpinUntil(condition, timeout ?? ConditionTimeout);

        [Test]
        public async Task AutoRefreshThenFilter_ConcurrentAddsAndPropertyActivation_AllItemsObserved()
        {
            // One adder thread sequentially adds items to the cache while a single flipper thread
            // concurrently sets each item's Activated to true. Final filter contents must include
            // every item (every item ends Activated=true).
            //
            // KeyedActivable's setter only raises PropertyChanged on actual value change, so a
            // dropped false->true transition is unrecoverable.
            //
            // AutoRefresh must attach each item's handler before the Add reaches Filter.
            // Otherwise activation between Filter's initial read and handler attachment is lost.
            const int iterations = 100;
            const int itemCount = 200;

            for (var iter = 0; iter < iterations; iter++)
            {
                using var cache = new SourceCache<KeyedActivable, int>(x => x.Id);
                var items = Enumerable.Range(0, itemCount).Select(i => new KeyedActivable(i)).ToList();

                using var results = cache.Connect()
                    .AutoRefresh(x => x.Activated)
                    .Filter(x => x.Activated)
                    .AsAggregator();

                using var barrier = new Barrier(2);

                var adder = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    foreach (var item in items) cache.AddOrUpdate(item);
                });

                var flipper = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    foreach (var item in items) item.Activated = true;
                });

                await Task.WhenAll(adder, flipper).WaitAsync(ConditionTimeout);

                var expected = items.Select(x => x.Id).ToHashSet();
                WaitForCondition(() => results.Data.Keys.ToHashSet().SetEquals(expected));

                var actual = results.Data.Keys.ToHashSet();
                await Assert.That(actual).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Any).Because("concurrent observations must match expected state");
                await Assert.That(results.Error).IsNull().Because($"iter {iter}: pipeline must not error");
            }
        }

        [Test]
        public async Task AutoRefreshThenFilter_DualSubscribers_AllItemsObserved()
        {
            // Two independent cache subscribers running on the ThreadPool:
            //   Sub 1 (mutator): on every Add change, flips item.Activated to true
            //   Sub 2 (filter chain): AutoRefresh + Filter (filter = Activated)
            // Items start with Activated=false (filtered out). The mutator flips every item, so
            // the final filter contents must include every item.
            //
            // These independent deliveries must converge regardless of whether activation
            // happens before handler attachment or while Filter processes the initial Add.
            const int iterations = 100;
            const int itemCount = 200;

            for (var iter = 0; iter < iterations; iter++)
            {
                using var cache = new SourceCache<KeyedActivable, int>(x => x.Id);
                var items = Enumerable.Range(0, itemCount).Select(i => new KeyedActivable(i)).ToList();

                using var mutator = cache.Connect()
                    .ObserveOn(TaskPoolScheduler.Default)
                    .Subscribe(changes =>
                    {
                        foreach (var change in changes)
                        {
                            if (change.Reason == ChangeReason.Add)
                            {
                                change.Current.Activated = true;
                            }
                        }
                    });

                using var results = cache.Connect()
                    .ObserveOn(TaskPoolScheduler.Default)
                    .AutoRefresh(x => x.Activated)
                    .Filter(x => x.Activated)
                    .AsAggregator();

                foreach (var item in items) cache.AddOrUpdate(item);

                var expected = items.Select(x => x.Id).ToHashSet();
                WaitForCondition(() => results.Data.Keys.ToHashSet().SetEquals(expected));

                var actual = results.Data.Keys.ToHashSet();
                await Assert.That(actual).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Any).Because("concurrent observations must match expected state");
                await Assert.That(results.Error).IsNull().Because($"iter {iter}: pipeline must not error");
            }
        }

        [Test]
        public async Task AutoRefreshThenFilter_PropertyActivationDuringInitialDelivery_IsObserved()
        {
            using var cache = new SourceCache<KeyedActivable, int>(x => x.Id);
            var item = new KeyedActivable(1);
            using var propertyRead = new ManualResetEventSlim();
            using var activationFinished = new ManualResetEventSlim();

            var flipper = Task.Factory.StartNew(
                () =>
                {
                    try
                    {
                        if (!propertyRead.Wait(ConditionTimeout))
                        {
                            throw new TimeoutException("Filter did not read the initial property value.");
                        }

                        item.Activated = true;
                    }
                    finally
                    {
                        activationFinished.Set();
                    }
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            try
            {
                using var results = cache.Connect()
                    .AutoRefresh(x => x.Activated)
                    .Filter(current =>
                    {
                        var activated = current.Activated;
                        if (!activated)
                        {
                            propertyRead.Set();
                            if (!activationFinished.Wait(ConditionTimeout))
                            {
                                throw new TimeoutException("Property activation did not finish during initial delivery.");
                            }
                        }

                        return activated;
                    })
                    .AsAggregator();

                cache.AddOrUpdate(item);
                await flipper.WaitAsync(ConditionTimeout);

                await Assert.That(results.Data.Keys).IsEquivalentTo(new[] { item.Id }, TUnit.Assertions.Enums.CollectionOrdering.Any);
                await Assert.That(results.Error).IsNull();
            }
            finally
            {
                propertyRead.Set();
                await flipper.WaitAsync(ConditionTimeout);
            }
        }
    }
}
