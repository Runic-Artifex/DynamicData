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
        public static partial class WithSourceComparer
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
                    using var sub = marketCache.Connect().MergeManyChangeSets(factory, Market.RatingCompare).Subscribe();

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
                    using var sub = marketCache.Connect().MergeManyChangeSets(factory, Market.RatingCompare).Subscribe();

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

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets.Select((m, index) => new { Market = m, Index = index }).ForEach(m => m.Market.SetPrices(m.Index * ItemIdStride, (m.Index * ItemIdStride) + PricesPerMarket, () => GetRandomPrice(randomizer)));

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
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AllNewSubItemsPresentInResult()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    marketCache.AddOrUpdate(markets);

                    // when
                    markets.Select((m, index) => new { Market = m, Index = index }).ForEach(m => m.Market.SetPrices(m.Index * ItemIdStride, (m.Index * ItemIdStride) + PricesPerMarket, () => GetRandomPrice(randomizer)));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(markets.Sum(m => m.PricesCache.Count)).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AllRefreshedSubItemsAreRefreshed()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets.Select((m, index) => new { Market = m, Index = index }).ForEach(m => m.Market.SetPrices(m.Index * ItemIdStride, (m.Index * ItemIdStride) + PricesPerMarket, () => GetRandomPrice(randomizer)));
                    marketCache.AddOrUpdate(markets);

                    // when
                    markets.ForEach(m => m.RefreshAllPrices(() => GetRandomPrice(randomizer)));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount + 1);
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

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets[0].Rating = 1.0;
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
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldBeNoOpWhenRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets[0].Rating = 1.0;
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
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldBeUnhiddenWhenOtherIsRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets[0].Rating = 1.0;
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
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AnyDuplicateValuesShouldNotRefreshWhenHidden()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets[0].Rating = 1.0;
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
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AnyRemovedSubItemIsRemoved()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    markets.Select((m, index) => new { Market = m, Index = index }).ForEach(m => m.Market.SetPrices(m.Index * ItemIdStride, (m.Index * ItemIdStride) + PricesPerMarket, () => GetRandomPrice(randomizer)));

                    // when
                    markets.ForEach(m => m.PricesCache.Edit(updater => updater.RemoveKeys(updater.Keys.Take(RemoveCount))));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount);
                    await Assert.That(results.Data.Count).IsEqualTo(MarketCount * (PricesPerMarket - RemoveCount));
                    await Assert.That(results.Messages.Count).IsEqualTo(MarketCount * 2);
                    await Assert.That(results.Messages[0].Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(MarketCount * RemoveCount);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task AnySourceItemRemovedRemovesAllSourceValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    marketCache.AddOrUpdate(markets);
                    markets.Select((m, index) => new { Market = m, Index = index }).ForEach(m => m.Market.SetPrices(m.Index * ItemIdStride, (m.Index * ItemIdStride) + PricesPerMarket, () => GetRandomPrice(randomizer)));

                    // when
                    marketCache.Edit(updater => updater.RemoveKeys(updater.Keys.Take(RemoveCount)));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(MarketCount - RemoveCount);
                    await Assert.That(results.Data.Count).IsEqualTo((MarketCount - RemoveCount) * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(MarketCount * PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(PricesPerMarket * RemoveCount);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task BestChoiceFromDuplicatesSelectedWhenChangeSetCreated()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var marketOriginal = new Market(0);
                    var marketBetter = new Market(1);
                    marketBetter.Rating = 1.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketBetter.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketBetter);

                    // when
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var resultsLow = ChangeSetByLowRating(marketCache, false).AsAggregator();

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLow.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task ChangingSourceByUpdateRemovesPreviousAndAddsNewValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
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
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var pair in results.Data.Items.Zip(updatedMarket.PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                }

                [Test]
                public async Task ChangingSourceByUpdateRemovesPreviousAndEmitsBetterValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    var market = new Market(0);
                    var marketWorse = new Market(1);
                    SetRating(marketCache, marketWorse, -1);
                    market.SetPrices(0, PricesPerMarket * 2, () => GetRandomPrice(randomizer));
                    marketWorse.SetPrices(0, PricesPerMarket * 2, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(market);
                    marketCache.AddOrUpdate(marketWorse);

                    var updatedMarket = new Market(market);
                    updatedMarket.SetPrices(PricesPerMarket, PricesPerMarket * 3, () => GetRandomPrice(randomizer));

                    // when
                    marketCache.AddOrUpdate(updatedMarket);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Take(PricesPerMarket).Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketWorse.Id);
                    }
                    foreach (var guid in results.Data.Items.Skip(PricesPerMarket).Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(updatedMarket.Id);
                    }
                }

                [Test]
                public async Task ChildComparerOnlyRefreshesVisibleValues()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var lowRatingLowPriceResults = ChangeSetByLowRatingThenLowPrice(marketCache, false).AsAggregator();
                    using var lowRatingHighPriceResults = ChangeSetByLowRatingThenHighPrice(marketCache, false).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketLowest = new Market(2);

                    marketLowest.Rating = marketLow.Rating = -1;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLow.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLowest.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(marketLowest);

                    // when
                    marketLowest.RefreshAllPrices(LowestPrice - 1);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(3);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                    await Assert.That(lowRatingLowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Refreshes).IsEqualTo(PricesPerMarket);
                    foreach (var guid in lowRatingLowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLowest.Id);
                    }
                    await Assert.That(lowRatingHighPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowRatingHighPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                }

                [Test]
                public async Task ChildComparerOnlyUpdatesVisibleValuesOnUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var lowRatingLowPriceResults = ChangeSetByLowRatingThenLowPrice(marketCache, false).AsAggregator();
                    using var lowRatingHighPriceResults = ChangeSetByLowRatingThenHighPrice(marketCache, false).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketLow = new Market(1);
                    var marketLowest = new Market(2);

                    marketLowest.Rating = marketLow.Rating = -1;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketLow.SetPrices(0, PricesPerMarket, LowestPrice);
                    marketLowest.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(marketLowest);

                    // when
                    marketLowest.UpdateAllPrices(LowestPrice - 1);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(3);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                    await Assert.That(lowRatingLowPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(lowRatingLowPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowRatingLowPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLowest.Id);
                    }
                    await Assert.That(lowRatingHighPriceResults.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(lowRatingHighPriceResults.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in lowRatingHighPriceResults.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLow.Id);
                    }
                }

                [Test]
                public async Task ChildComparerUpdatesToCorrectValueOnUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var resultsLow = ChangeSetByLowRating(marketCache, false).AsAggregator();
                    using var resultsLowPrice = ChangeSetByRatingThenLowPrice(marketCache, false).AsAggregator();
                    using var resultsHighPrice = ChangeSetByRatingThenHighPrice(marketCache, false).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketHighest = new Market(1);
                    var marketLowest = new Market(2);
                    marketLowest.Rating = marketHighest.Rating = 1.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketHighest.SetPrices(0, PricesPerMarket, HighestPrice);
                    marketLowest.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketHighest);
                    marketCache.AddOrUpdate(marketLowest);

                    // when
                    marketLowest.UpdateAllPrices(LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(3);
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLow.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }

                    await Assert.That(resultsLowPrice.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLowPrice.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLowPrice.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(resultsLowPrice.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLowPrice.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLowPrice.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketLowest.Id);
                    }

                    await Assert.That(resultsHighPrice.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsHighPrice.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsHighPrice.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsHighPrice.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsHighPrice.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsHighPrice.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketHighest.Id);
                    }
                }

                [Test]
                public async Task EqualityComparerAndChildComparerRefreshesBecomeUpdates()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var resultsLow = ChangeSetByLowRating(marketCache).AsAggregator();
                    using var resultsRecent = ChangeSetByRatingThenRecent(marketCache).AsAggregator();
                    using var resultsTimeStamp = ChangeSetByRatingThenTimeStamp(marketCache).AsAggregator();
                    var marketLow = new Market(0);
                    var market = new Market(1);
                    marketLow.Rating = -1;
                    marketLow.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    market.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(market);
                    market.SetPrices(0, PricesPerMarket, LowestPrice);
                    // Update again, but only the timestamp will change, so resultsRecent will ignore
                    market.SetPrices(0, PricesPerMarket, LowestPrice);

                    // when
                    // resultsRecent won't see the refresh because it ignored the update
                    // resultsTimeStamp will see the refreshes because it didn't
                    market.RefreshAllPrices(() => GetRandomPrice(randomizer));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Messages.Count).IsEqualTo(1);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Messages.Count).IsEqualTo(4);
                    await Assert.That(resultsRecent.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsRecent.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(resultsRecent.Summary.Overall.Refreshes).IsEqualTo(0);
                    await Assert.That(resultsTimeStamp.Messages.Count).IsEqualTo(5);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Refreshes).IsEqualTo(PricesPerMarket);
                }

                [Test]
                public async Task EqualityComparerAndChildComparerWorkTogetherForRefreshes()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var resultsLow = ChangeSetByLowRating(marketCache).AsAggregator();
                    using var resultsRecent = ChangeSetByRatingThenRecent(marketCache).AsAggregator();
                    using var resultsTimeStamp = ChangeSetByRatingThenTimeStamp(marketCache).AsAggregator();
                    var marketLow = new Market(0);
                    var market = new Market(1);
                    marketLow.Rating = -1;
                    marketLow.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    market.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(market);
                    market.SetPrices(0, PricesPerMarket, LowestPrice);
                    // Update again, but only the timestamp will change, so resultsRecent will ignore
                    market.SetPrices(0, PricesPerMarket, LowestPrice);

                    // when
                    // resultsRecent won't see the refresh because it ignored the update
                    // resultsTimeStamp will see the refreshes because it didn't
                    market.RefreshAllPrices(LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Messages.Count).IsEqualTo(1);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Messages.Count).IsEqualTo(4);
                    await Assert.That(resultsRecent.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsRecent.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(resultsRecent.Summary.Overall.Refreshes).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsTimeStamp.Messages.Count).IsEqualTo(5);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Refreshes).IsEqualTo(PricesPerMarket);
                }

                [Test]
                public async Task EqualityComparerAndChildComparerWorkTogetherForUpdates()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var resultsLow = ChangeSetByLowRating(marketCache).AsAggregator();
                    using var resultsRecent = ChangeSetByRatingThenRecent(marketCache).AsAggregator();
                    using var resultsTimeStamp = ChangeSetByRatingThenTimeStamp(marketCache).AsAggregator();
                    var marketLow = new Market(0);
                    var market = new Market(1);
                    marketLow.Rating = -1;
                    marketLow.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    market.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketLow);
                    marketCache.AddOrUpdate(market);
                    market.SetPrices(0, PricesPerMarket, LowestPrice);

                    // when
                    market.UpdateAllPrices(LowestPrice);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Messages.Count).IsEqualTo(1);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Messages.Count).IsEqualTo(3);
                    await Assert.That(resultsRecent.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsRecent.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsRecent.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(resultsRecent.Summary.Overall.Refreshes).IsEqualTo(0);
                    await Assert.That(resultsTimeStamp.Messages.Count).IsEqualTo(4);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 3);
                    await Assert.That(resultsTimeStamp.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task EqualityComparerHidesUpdatesWithoutChanges()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    // having
                    var market = new Market(0);
                    using var results = CreateChangeSet(marketCache, "Equality Compare", Market.RatingCompare, equalityComparer: MarketPrice.EqualityComparer, resortOnRefresh: true).AsAggregator();
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
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    marketCache.AddOrUpdate(Enumerable.Range(0, MarketCount).Select(n => new FixedMarket(() => GetRandomPrice(randomizer), n * ItemIdStride, (n * ItemIdStride) + PricesPerMarket)));

                    // when
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
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
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    marketCache.AddOrUpdate(Enumerable.Range(0, MarketCount).Select(n => new FixedMarket(() => GetRandomPrice(randomizer), n * ItemIdStride, (n * ItemIdStride) + PricesPerMarket, completable: completeChildren)));
                    var hasSourceSequenceCompleted = false;
                    var hasMergedSequenceCompleted = false;

                    using var cleanup = marketCache.Connect().Do(_ => { }, () => hasSourceSequenceCompleted = true)
                        .MergeManyChangeSets(m => m.LatestPrices, Market.RatingCompare).Subscribe(_ => { }, () => hasMergedSequenceCompleted = true);

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
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    // having
                    var markets = Enumerable.Range(0, MarketCount).Select(n => new Market(n)).ToArray();
                    marketCache.AddOrUpdate(markets);
                    var receivedError = default(Exception);
                    var expectedError = new Exception("Test exception");
                    var throwObservable = Observable.Throw<IChangeSet<IMarket, Guid>>(expectedError);

                    using var cleanup = marketCache.Connect().Concat(throwObservable)
                        .MergeManyChangeSets(m => m.LatestPrices, Market.RatingCompare).Subscribe(_ => { }, err => receivedError = err);

                    // when
                    marketCache.Items.ForEach(m => (m as IDisposable)?.Dispose());
                    marketCache.Dispose();

                    // then
                    await Assert.That(receivedError).IsEqualTo(expectedError);
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
                    var actionParentCompare1 = () => emptyChangeSetObs.MergeManyChangeSets(nullSelector, sourceComparer: emptyParentComparer);
                    var actionParentCompareKey1a = () => nullChangeSetObs.MergeManyChangeSets(emptyKeySelector, sourceComparer: emptyParentComparer);
                    var actionParentCompareKey1b = () => emptyChangeSetObs.MergeManyChangeSets(nullKeySelector, sourceComparer: emptyParentComparer);
                    var actionParentCompareKey1c = () => emptyChangeSetObs.MergeManyChangeSets(emptyKeySelector, sourceComparer: nullParentComparer);
                    var actionParentCompare2 = () => emptyChangeSetObs.MergeManyChangeSets(nullSelector, sourceComparer: emptyParentComparer, equalityComparer: emptyEqualityComparer);
                    var actionParentCompareKey2a = () => nullChangeSetObs.MergeManyChangeSets(emptyKeySelector, sourceComparer: emptyParentComparer, equalityComparer: emptyEqualityComparer);
                    var actionParentCompareKey2b = () => emptyChangeSetObs.MergeManyChangeSets(nullKeySelector, sourceComparer: emptyParentComparer, equalityComparer: emptyEqualityComparer);

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

                    await Assert.That(actionParentCompare1).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompareKey1a).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompareKey1b).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompareKey1c).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompare2).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompareKey2a).Throws<ArgumentNullException>();
                    await Assert.That(actionParentCompareKey2b).Throws<ArgumentNullException>();
                }

                [Test]
                public async Task OnlyAddsBetterValuesOnSourceUpdate()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var resultsLow = ChangeSetByLowRating(marketCache, false).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketBetter = new Market(1);
                    marketBetter.Rating = 1.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketBetter);

                    // when
                    marketBetter.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLow.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task OnlyUpdatesOnDuplicateIfNewItemIsFromBetterParent()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var resultsLow = ChangeSetByLowRating(marketCache, false).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketBetter = new Market(1);
                    marketBetter.Rating = 1.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketBetter.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);

                    // when
                    marketCache.AddOrUpdate(marketBetter);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLow.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }

                [Test]
                public async Task SourceRefreshDoesNothingIfDisabled()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache, resortOnRefresh: false).AsAggregator();
                    markets[0].Rating = 1.0;
                    marketCache.AddOrUpdate(markets);
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // when
                    SetRating(marketCache, markets[1], 2.0);

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
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task SourceRefreshGeneratesUpdatesAsNeeded()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var markets = Enumerable.Range(0, 2).Select(n => new Market(n)).ToArray();
                    using var results = ChangeSetByRating(marketCache).AsAggregator();
                    markets[0].Rating = 1.0;
                    marketCache.AddOrUpdate(markets);
                    markets[0].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    markets[1].SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));

                    // when
                    SetRating(marketCache, markets[1], 2.0);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    foreach (var pair in results.Data.Items.Zip(markets[1].PricesCache.Items))
                    {
                        await Assert.That(pair.First).IsEqualTo(pair.Second);
                    }
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                }

                [Test]
                public async Task UpdatesToCorrectValueOnRefresh()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();
                    using var resultsLow = ChangeSetByLowRating(marketCache, false).AsAggregator();
                    using var resultsRefresh = ChangeSetByRating(marketCache, true).AsAggregator();
                    using var resultsLowRefresh = ChangeSetByLowRating(marketCache, true).AsAggregator();
                    var marketOriginal = new Market(0);
                    var marketBetter = new Market(1);
                    marketBetter.Rating = -1.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketBetter.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketBetter);

                    // when
                    SetRating(marketCache, marketBetter, 2.0);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(marketCacheResults.Summary.Overall.Refreshes).IsEqualTo(1);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                    await Assert.That(resultsLow.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLow.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLow.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLow.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                    await Assert.That(resultsRefresh.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsRefresh.Summary.Overall.Updates).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsRefresh.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsRefresh.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsRefresh.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                    await Assert.That(resultsLowRefresh.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLowRefresh.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(resultsLowRefresh.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(resultsLowRefresh.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(resultsLowRefresh.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in resultsLowRefresh.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketOriginal.Id);
                    }
                }
                [Test]
                public async Task UpdatesToCorrectValueOnRemove()
                {
                    using var marketCache = new SourceCache<IMarket, Guid>(p => p.Id);
                    using var marketCacheResults = marketCache.Connect().AsAggregator();

                    var randomizer = new Randomizer(0x10012022);

                    // having
                    var marketOriginal = new Market(0);
                    var marketBetter = new Market(1);
                    var marketBest = new Market(2);
                    marketBetter.Rating = 1.0;
                    marketBest.Rating = 5.0;
                    marketOriginal.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketBetter.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketBest.SetPrices(0, PricesPerMarket, () => GetRandomPrice(randomizer));
                    marketCache.AddOrUpdate(marketOriginal);
                    marketCache.AddOrUpdate(marketBest);
                    marketCache.AddOrUpdate(marketBetter);
                    using var results = ChangeSetByRating(marketCache, false).AsAggregator();

                    // when
                    marketCache.Remove(marketBest);

                    // then
                    await Assert.That(marketCacheResults.Data.Count).IsEqualTo(2);
                    await Assert.That(results.Data.Count).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Adds).IsEqualTo(PricesPerMarket);
                    await Assert.That(results.Summary.Overall.Updates).IsEqualTo(PricesPerMarket * 2);
                    await Assert.That(results.Summary.Overall.Removes).IsEqualTo(0);
                    await Assert.That(results.Summary.Overall.Refreshes).IsEqualTo(0);
                    foreach (var guid in results.Data.Items.Select(cp => cp.MarketId))
                    {
                        await Assert.That(guid).IsEqualTo(marketBetter.Id);
                    }
                }

                private static IObservable<IChangeSet<MarketPrice, int>> CreateChangeSet(
                        SourceCache<IMarket, Guid> marketCache,
                        string name,
                        IComparer<IMarket>? sourceComp = null,
                        IComparer<MarketPrice>? childCompare = null,
                        IEqualityComparer<MarketPrice>? equalityComparer = null,
                        bool resortOnRefresh = true) =>
                    marketCache.Connect()
                        .DebugSpy(name)
                        .MergeManyChangeSets(m => m.LatestPrices.DebugSpy($"{name} [{m.Name} Prices]"), sourceComp ?? Market.RatingCompare, resortOnSourceRefresh: resortOnRefresh, equalityComparer, childCompare)
                        .DebugSpy($"{name} [Results]");

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByRating(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Rating",
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByRatingThenHighPrice(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Rating | High",
                        sourceComp: Market.RatingCompare,
                        childCompare: MarketPrice.HighPriceCompare,
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByRatingThenLowPrice(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Rating | Low",
                        sourceComp: Market.RatingCompare,
                        childCompare: MarketPrice.LowPriceCompare,
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByRatingThenRecent(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Rating | Recent",
                        sourceComp: Market.RatingCompare,
                        childCompare: MarketPrice.LatestPriceCompare,
                        equalityComparer: MarketPrice.EqualityComparer,
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByRatingThenTimeStamp(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Rating | Timestamp",
                        sourceComp: Market.RatingCompare,
                        childCompare: MarketPrice.LatestPriceCompare,
                        equalityComparer: MarketPrice.EqualityComparerWithTimeStamp,
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByLowRating(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Low Rating",
                        sourceComp: Market.RatingCompare.Invert(),
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByLowRatingThenHighPrice(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Low Rating | High",
                        sourceComp: Market.RatingCompare.Invert(),
                        childCompare: MarketPrice.HighPriceCompare,
                        resortOnRefresh: resortOnRefresh);

                private static IObservable<IChangeSet<MarketPrice, int>> ChangeSetByLowRatingThenLowPrice(
                        SourceCache<IMarket, Guid> marketCache,
                        bool resortOnRefresh = true)
                    => CreateChangeSet(
                        marketCache: marketCache,
                        name: "Low Rating | Low",
                        sourceComp: Market.RatingCompare.Invert(),
                        childCompare: MarketPrice.LowPriceCompare,
                        resortOnRefresh: resortOnRefresh);

                private static IMarket SetRating(
                    SourceCache<IMarket, Guid> marketCache,
                    IMarket market,
                    double newRating)
                {
                    market.Rating = newRating;
                    marketCache.Refresh(market);
                    return market;
                }

            }
        }
    }
}
