#if REACTIVE_TESTS
using DynamicData.Reactive.Kernel;
#else
using DynamicData.Kernel;
#endif
using System;
using System.Collections.Generic;
using System.Linq;

using Bogus;

using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public static partial class MergeManyChangeSetsFixture
{
    public static partial class ForCacheChangeSets
    {
        public static partial class WithoutSourceComparer
        {
            public class UnitTests
            {
                [Test]
                public async Task AbleToInvokeFactory()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    // having
                    var invoked = false;
                    IObservable<IChangeSet<MarketPrice, int>> factory(IMarket m)
                    {
                        invoked = true;
                        return m.LatestPrices;
                    }
                    using var sub = marketCache.Connect().MergeManyChangeSets(factory).Subscribe();

                    // when
                    marketCache.AddOrUpdate(new Market(0));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(1);
                    await Assert.That(invoked).IsTrue();
                }

                [Test]
                public async Task AbleToInvokeFactoryWithKey()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    // having
                    var invoked = false;
                    IObservable<IChangeSet<MarketPrice, int>> factory(IMarket m, Guid g)
                    {
                        invoked = true;
                        return m.LatestPrices;
                    }
                    using var sub = marketCache.Connect().MergeManyChangeSets(factory).Subscribe();

                    // when
                    marketCache.AddOrUpdate(new Market(0));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(1);
                    await Assert.That(invoked).IsTrue();
                }

                [Test]
                public async Task AllExistingSubItemsPresentInResult()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    AddUniquePrices(markets, randomizer);

                    // when
                    marketCache.AddOrUpdate(markets);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(markets.Sum(m => m.PricesCache.Count)).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(1);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                }

                [Test]
                public async Task AllNewSubItemsPresentInResult()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);

                    // when
                    AddUniquePrices(markets, randomizer);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(markets.Sum(m => m.PricesCache.Count)).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                }

                [Test]
                public async Task AllRefreshedSubItemsAreRefreshed()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    AddUniquePrices(markets, randomizer);

                    // when
                    markets.ForEach(m => m.RefreshAllPrices(() => GetRandomPrice(randomizer)));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount * 2);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(MarketCount * PricesPerMarket);
                }

                [Test]
                public async Task AnyDuplicateKeyValuesShouldBeHidden()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);

                    // when
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    foreach (var pair in results.Data.Items.Zip(markets[0].PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldBeNoOpWhenRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // when
                    markets[1].RemoveAllPrices();

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    foreach (var pair in results.Data.Items.Zip(markets[0].PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldBeUnhiddenWhenOtherIsRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // when
                    marketCache.Remove(markets[0]);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(1);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    foreach (var pair in results.Data.Items.Zip(markets[1].PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                    await Assert.That(results.Messages.Count).IsEqualTo(2);
                    await Assert.That(results.Messages[1].Updates).IsEqualTo(PricesPerMarket);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldNotRefreshWhenHidden()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // when
                    markets[1].RefreshAllPrices(() => GetRandomPrice(randomizer));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var pair in results.Data.Items.Zip(markets[0].PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                }

                [Test]
                public async Task AnyRemovedSubItemIsRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    AddUniquePrices(markets, randomizer);

                    // when
                    markets.ForEach(m => m.PricesCache.Edit(updater => updater.RemoveKeys(updater.Keys.Take(RemoveCount))));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * (PricesPerMarket - RemoveCount));
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount * 2);
                    await Assert.That(results.Messages[0].Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(MarketCount * RemoveCount);
                }

                [Test]
                public async Task AnySourceItemRemovedRemovesAllSourceValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    AddUniquePrices(markets, randomizer);
                    marketCache.AddOrUpdate(markets);

                    // when
                    marketCache.Edit(updater => updater.RemoveKeys(updater.Keys.Take(RemoveCount)));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount - RemoveCount);
                    await Assert.That(results.Messages.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo((MarketCount - RemoveCount) * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(PricesPerMarket * RemoveCount);
                }

                [Test]
                public async Task ChangingSourceByUpdateRemovesPreviousAndAddsNewValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    var market = new Market(0);
                    market.SetPrices(0, PricesPerMarket * 2, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(market);
                    var updatedMarket = new Market(market);
                    updatedMarket.SetPrices(PricesPerMarket, PricesPerMarket * 3, () => GetRandomPrice(randomizer));

                    // when
                    marketCache.AddOrUpdate(updatedMarket);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(1);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(PricesPerMarket);
                    foreach (var pair in results.Data.Items.Zip(updatedMarket.PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                }

                [Test]
                public async Task ClearingParentEmitsSingleChangeSet()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    AddUniquePrices(markets, randomizer);
                    marketCache.AddOrUpdate(markets);

                    // when
                    marketCache.Clear();

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(0);
                    await Assert.That(results.Data.Count).IsEqualTo(0);
                    await Assert.That(results.Messages.Count).IsEqualTo(2);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                }

                [Test]
                public async Task ComparerOnlyAddsBetterAddedValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketHigh = new Market(2);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(marketHigh);

                    // when
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketHigh.SetPrices(0, PricesPerMarket, HighestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(3);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketHigh.Id);
                    }
                }

                [Test]
                public async Task ComparerOnlyAddsBetterExistingValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketHigh = new Market(2);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketHigh.SetPrices(0, PricesPerMarket, HighestPrice);

                    // when
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(marketHigh);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(3);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketHigh.Id);
                    }
                }

                [Test]
                public async Task ComparerOnlyAddsBetterValuesOnSourceUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketLowLow = new Market(marketLow);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketLowLow.SetPrices(0, PricesPerMarket, LowestPrice - 1);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);

                    // when
                    marketCache.AddOrUpdate(marketLowLow);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLowLow.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(0);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task ComparerOnlyRefreshesVisibleValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);

                    // when
                    marketLow.RefreshAllPrices(LowestPrice - 1);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Refreshes).IsEqualTo(PricesPerMarket);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task ComparerOnlyUpdatesVisibleValuesOnUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);

                    // when
                    marketLow.UpdateAllPrices(LowestPrice - 1);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(lowPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task ComparerUpdatesToCorrectValueOnRefresh()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketFlipFlop = new Market(1);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketFlipFlop.SetPrices(0, PricesPerMarket, HighestPrice);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketFlipFlop);

                    // when
                    marketFlipFlop.RefreshAllPrices(LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketFlipFlop.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(highPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task ComparerUpdatesToCorrectValueOnRemove()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketHigh = new Market(2);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(marketHigh);
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketHigh.SetPrices(0, PricesPerMarket, HighestPrice);

                    // when
                    marketCache.Remove(marketLow);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketHigh.Id);
                    }
                }

                [Test]
                public async Task ComparerUpdatesToCorrectValueOnUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    using var highPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.HighPriceCompare).AsAggregator();
                    using var lowPriceResults = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.LowPriceCompare).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketFlipFlop = new Market(1);
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketFlipFlop.SetPrices(0, PricesPerMarket, HighestPrice);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketFlipFlop);

                    // when
                    marketFlipFlop.UpdateAllPrices(LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(lowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketFlipFlop.Id);
                    }
                    await Assert.That(highPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(highPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(highPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(highPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in highPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task EqualityComparerHidesUpdatesWithoutChanges()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    // having
                    var market = new Market(0);
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    market.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketCache.AddOrUpdate(market);

                    // when
                    market.SetPrices(0, PricesPerMarket, LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(1);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(1);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task EveryItemVisibleWhenSequenceCompletes()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    marketCache.AddOrUpdate(Enumerable.Range(0, MarketCount).Select(n => new FixedMarket(() => GetRandomPrice(randomizer), n * ItemIdStride, (n * ItemIdStride) + PricesPerMarket)));

                    // when
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices).AsAggregator();

                    marketCache.Items.ForEach(m => (m as IDisposable)?.Dispose());
                    marketCache.Dispose();

                    // then
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket * MarketCount);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket * MarketCount);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                [Arguments(false, false)]
                [Arguments(false, true)]
                [Arguments(true, false)]
                [Arguments(true, true)]
                public async Task MergedObservableCompletesOnlyWhenSourceAndAllChildrenComplete(bool completeSource, bool completeChildren)
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    marketCache.AddOrUpdate(Enumerable.Range(0, MarketCount).Select(n => new FixedMarket(() => GetRandomPrice(randomizer), n * ItemIdStride, (n * ItemIdStride) + PricesPerMarket, completable: completeChildren)));
                    var hasSourceSequenceCompleted = false;
                    var hasMergedSequenceCompleted = false;

                    using var cleanup = marketCache.Connect().Do(_ => { }, () => hasSourceSequenceCompleted = true)
                        .MergeManyChangeSets(m => m.LatestPrices).Subscribe(_ => { }, () => hasMergedSequenceCompleted = true);

                    // when
                    if (completeSource)
                    {
                        marketCache.Items.ForEach(m => (m as IDisposable)?.Dispose());
                        marketCache.Dispose();
                    }

                    // then
                    await Assert.That(hasSourceSequenceCompleted).IsEqualTo(completeSource);
                    await Assert.That(hasMergedSequenceCompleted).IsEqualTo(completeSource && completeChildren);
                }

                [Test]
                public async Task MergedObservableWillFailIfSourceFails()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    marketCache.AddOrUpdate(markets);
                    var receivedError = default(Exception);
                    var expectedError = new Exception("Test exception");
                    var throwObservable = Observable.Throw<IChangeSet<IMarket, Guid>>(expectedError);

                    using var cleanup = marketCache.Connect().Concat(throwObservable)
                        .MergeManyChangeSets(m => m.LatestPrices).Subscribe(_ => { }, err => receivedError = err);

                    // when
                    marketCache.Items.ForEach(m => (m as IDisposable)?.Dispose());
                    marketCache.Dispose();

                    // then
                    await Assert.That(receivedError).IsEqualTo(expectedError);
                }

                [Test]
                public async Task MergeManyChangeSetsWorksCorrectlyWithValueTypes()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);

                    var randomizer = new Randomizer(0x21123737);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    marketCache.AddOrUpdate(markets);
                    markets.ForEach(m => m.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer)));
                    using var results = marketCache.Connect()
                        .MergeManyChangeSets(m => m.LatestPrices.Transform(p => p.Price))
                        .AsAggregator();

                    // when
                    markets.ForEach(m => m.RemoveAllPrices());

                    // then
                    await Assert.That(results.Data.Count).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(PricesPerMarket);
                }

                [Test]
                public async Task NullChecks()
                {
                    // having
                    var emptyChangeSetObs = Observable.Empty<IChangeSet<int, int>>();
                    var nullChangeSetObs = (IObservable<IChangeSet<int, int>>)null!;
                    var emptyChildChangeSetObs = Observable.Empty<IChangeSet<string, string>>();
                    var emptySelector = new Func<int, IObservable<IChangeSet<string, string>>>(i => emptyChildChangeSetObs);
                    var emptyKeySelector = new Func<int, int, IObservable<IChangeSet<string, string>>>((i, key) => emptyChildChangeSetObs);
                    var nullSelector = (Func<int, IObservable<IChangeSet<string, string>>>)null!;
                    var nullKeySelector = (Func<int, int, IObservable<IChangeSet<string, string>>>)null!;
                    var nullParentComparer = (IComparer<int>)null!;
                    var emptyParentComparer = new NoOpComparer<int>() as IComparer<int>;
                    var nullChildComparer = (IComparer<string>)null!;
                    var emptyChildComparer = new NoOpComparer<string>() as IComparer<string>;
                    var nullEqualityComparer = (IEqualityComparer<string>)null!;
                    var emptyEqualityComparer = new NoOpEqualityComparer<string>() as IEqualityComparer<string>;

                    // when
                    var actionDefault1 = () => emptyChangeSetObs.MergeManyChangeSets(nullSelector);
                    var actionDefault2a = () => nullChangeSetObs.MergeManyChangeSets(emptyKeySelector);
                    var actionDefault2b = () => emptyChangeSetObs.MergeManyChangeSets(nullKeySelector);
                    var actionChildCompare1 = () => emptyChangeSetObs.MergeManyChangeSets(nullSelector, comparer: emptyChildComparer);
                    var actionChildCompare2a = () => nullChangeSetObs.MergeManyChangeSets(emptyKeySelector, comparer: emptyChildComparer);
                    var actionChildCompare2b = () => emptyChangeSetObs.MergeManyChangeSets(nullKeySelector, comparer: emptyChildComparer);
                    var actionChildCompare2c = () => emptyChangeSetObs.MergeManyChangeSets(emptyKeySelector, comparer: nullChildComparer);

                    // then
                    await Assert.That(emptyChangeSetObs).IsNotNull();
                    await Assert.That(emptyChildChangeSetObs).IsNotNull();
                    await Assert.That(emptyChildComparer).IsNotNull();
                    await Assert.That(emptyEqualityComparer).IsNotNull();
                    await Assert.That(emptyKeySelector).IsNotNull();
                    await Assert.That(emptyParentComparer).IsNotNull();
                    await Assert.That(emptySelector).IsNotNull();
                    await Assert.That(nullChangeSetObs).IsNull();
                    await Assert.That(nullChildComparer).IsNull();
                    await Assert.That(nullEqualityComparer).IsNull();
                    await Assert.That(nullKeySelector).IsNull();
                    await Assert.That(nullParentComparer).IsNull();
                    await Assert.That(nullSelector).IsNull();

                    await Assert.That(actionDefault1).Throws<ArgumentNullException>();
                    await Assert.That(actionDefault2a).Throws<ArgumentNullException>();
                    await Assert.That(actionDefault2b).Throws<ArgumentNullException>();
                    await Assert.That(actionChildCompare1).Throws<ArgumentNullException>();
                    await Assert.That(actionChildCompare2a).Throws<ArgumentNullException>();
                    await Assert.That(actionChildCompare2b).Throws<ArgumentNullException>();
                    await Assert.That(actionChildCompare2c).Throws<ArgumentNullException>();
                }

                [Test]
                [Arguments(true)]
                [Arguments(false)]
                public async Task OrderOfChangesIsPreserved(bool removeFirst)
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);

                    var randomizer = new Randomizer(0x21123737);

                    // Arrange
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    AddUniquePrices(markets, randomizer);
                    marketCache.AddOrUpdate(markets);
                    var markets2 = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    AddUniquePrices(markets2, randomizer);
                    using var results = marketCache.Connect().MergeManyChangeSets(m => m.LatestPrices, MarketPrice.EqualityComparer).AsAggregator();
                    (var firstReason, var nextReason, int expectedChanges) = removeFirst
                        ? (ChangeReason.Remove, ChangeReason.Add, 2 * MarketCount * PricesPerMarket)
                        : (ChangeReason.Add, ChangeReason.Remove, 3 * MarketCount * PricesPerMarket);

                    // Act
                    marketCache.Edit(updater =>
                    {
                        if (removeFirst)
                        {
                            updater.Clear();
                            updater.AddOrUpdate(markets2);
                        }
                        else
                        {

                            updater.AddOrUpdate(markets2);
                            updater.Clear();
                        }
                    });

                    // Assert
                    await Assert.That(results.Messages.Count).IsEqualTo(2);
                    await Assert.That(results.Messages[0].All(change => change.Reason is ChangeReason.Add)).IsTrue();
                    await Assert.That(results.Messages[1].Count).IsEqualTo(expectedChanges);
                    await Assert.That(results.Messages[1].Take(MarketCount * PricesPerMarket).All(change => change.Reason == firstReason)).IsTrue();
                    await Assert.That(results.Messages[1].Skip(MarketCount * PricesPerMarket).All(change => change.Reason == nextReason)).IsTrue();
                }

                private static void AddUniquePrices(
                        Market[] markets,
                        Randomizer randomizer)
                    => markets.ForEach(m => m.AddUniquePrices(PricesPerMarket, _ => GetRandomPrice(randomizer)));
            }
        }
    }
}
