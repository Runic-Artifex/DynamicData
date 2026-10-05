#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public partial class AvgFixture
{
    public sealed class Contracts
    {
        [Test]
        public async Task DirectCacheMixedRefreshAndMembershipChangesUseStoredProjections()
        {
            using var source = new TestSourceCache<Measurement, int>(item => item.Key);
            var first = new Measurement(1, 10);
            var second = new Measurement(2, 20);
            source.AddOrUpdate(new[] { first, second });
            var selectorCalls = 0;
            using var subscription = source.Connect().Avg(item => { selectorCalls++; return item.Value; }, emptyValue: -1)
                .RecordValues(out var results);
            first.Value = 40;
            source.Edit(updater =>
            {
                updater.Refresh(first);
                updater.Remove(second);
                updater.AddOrUpdate(new Measurement(3, 60));
            });
            first.Value = 400;
            source.Remove(first);

            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 15.0, 50.0, 60.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(selectorCalls).IsEqualTo(4).Because("only additions and explicit refreshes invoke the selector");
            await Assert.That(results.Error).IsNull();
        }

        [Test]
        public async Task DirectListMovesAndIndexedRefreshKeepSeparateRepeatedReferenceProjections()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Measurement>>();
            var repeated = new Measurement(1, 10);
            var other = new Measurement(2, 100);
            var selectorCalls = 0;
            using var subscription = source.Avg(item => { selectorCalls++; return item.Value; }, emptyValue: -1)
                .RecordValues(out var results);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, repeated) });
            repeated.Value = 20;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, repeated) });
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, other, 1) });
            repeated.Value = 30;
            source.OnNext(new ChangeSet<Measurement>
            {
                new(repeated, 2, 0),
                new(ListChangeReason.Refresh, repeated, 1),
                new(ListChangeReason.Remove, repeated, 2),
                new(ListChangeReason.Add, new Measurement(3, 50), 0)
            });
            await Assert.That(results.RecordedValues[^1]).IsEqualTo(60.0);
            repeated.Value = 300;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.RemoveRange, new[] { repeated }) });
            await Assert.That(results.RecordedValues[^1]).IsEqualTo(75.0);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Clear, new[] { other, new Measurement(3, 50) }) });

            await Assert.That(results.RecordedValues[^1]).IsEqualTo(-1.0);
            await Assert.That(results.RecordedValues.Count).IsEqualTo(7);
            await Assert.That(selectorCalls).IsEqualTo(5);
            await Assert.That(results.Error).IsNull();
        }

        [Test]
        public async Task EmptyAggregateBatchInvalidatesAllOccurrencesIncludingRepeatedReferences()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Measurement>>();
            var repeated = new Measurement(1, 10);
            var selectorCalls = 0;
            using var subscription = source.ForAggregation().Avg(item => { selectorCalls++; return item.Value; }, emptyValue: -1)
                .RecordValues(out var results);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, repeated) });
            repeated.Value = 20;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, repeated) });
            repeated.Value = 40;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Refresh, repeated, 0) });
            repeated.Value = 400;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Remove, repeated, 0) });

            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { -1.0, 10.0, 15.0, 40.0, 40.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(selectorCalls).IsEqualTo(4);
            await Assert.That(results.Error).IsNull();
        }

        [Test]
        public async Task AggregateMixedRefreshMetadataCannotBeRecoveredButDirectOverloadHandlesIt()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Measurement>>();
            var first = new Measurement(1, 10);
            var second = new Measurement(2, 20);
            using var aggregate = source.ForAggregation().Avg(item => item.Value, emptyValue: -1).RecordValues(out var aggregateResults);
            using var direct = source.Avg(item => item.Value, emptyValue: -1).RecordValues(out var directResults);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.AddRange, new[] { first, second }) });
            first.Value = 40;
            source.OnNext(new ChangeSet<Measurement>
            {
                new(ListChangeReason.Refresh, first, 0),
                new(ListChangeReason.Add, new Measurement(3, 60))
            });
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Refresh, second, 1) });

            await Assert.That(aggregateResults.RecordedValues).IsEquivalentTo(new[] { -1.0, 15.0, 30.0, 40.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(directResults.RecordedValues).IsEquivalentTo(new[] { -1.0, 15.0, 40.0, 40.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(aggregateResults.Error).IsNull();
            await Assert.That(directResults.Error).IsNull();
        }

        [Test]
        public async Task AggregateRemovalPrefersReferenceIdentityAndSubtractsCachedValue()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Measurement>>();
            var first = new Measurement(1, 10);
            var equal = new Measurement(1, 30);
            using var subscription = source.ForAggregation().Avg(item => item.Value, emptyValue: -1).RecordValues(out var results);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.AddRange, new[] { first, equal }) });
            equal.Value = 300;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Remove, equal) });
            first.Value = 100;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Remove, first) });

            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { -1.0, 20.0, 10.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(results.Error).IsNull();
        }

        [Test]
        public async Task EqualProjectionReplacementTransfersAggregateOccurrenceOwnership()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Measurement>>();
            var previous = new Measurement(1, 10);
            var replacement = new Measurement(1, 10);
            using var subscription = source.ForAggregation().Avg(item => item.Value, emptyValue: -1).RecordValues(out var results);
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Add, previous) });
            source.OnNext(new ChangeSet<Measurement>
            {
                new(ListChangeReason.Replace, replacement, ReactiveUI.Primitives.Optional<Measurement>.Create(previous), 0, 0)
            });
            previous.Value = 100;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Refresh, replacement, 0) });
            replacement.Value = 40;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Refresh, replacement, 0) });
            replacement.Value = 400;
            source.OnNext(new ChangeSet<Measurement> { new(ListChangeReason.Remove, replacement) });

            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { -1.0, 10.0, 10.0, 10.0, 40.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(results.Error).IsNull();
        }

        [Test]
        [Arguments(false, false)]
        [Arguments(false, true)]
        [Arguments(true, false)]
        [Arguments(true, true)]
        public async Task SynchronousSnapshotSelectorFailureEmitsOnlyErrorAndReleasesSource(bool keyed, bool aggregate)
        {
            var expected = new InvalidOperationException("selector failed");
            var acquired = 0;
            var released = 0;
            Func<Measurement, int> selector = _ => throw expected;
            IObservable<double> averages;
            if (keyed)
            {
                var source = Cold<IChangeSet<Measurement, int>>(new ChangeSet<Measurement, int> { new(ChangeReason.Add, 1, new Measurement(1, 10)) });
                averages = aggregate ? source.ForAggregation().Avg(selector, -1) : source.Avg(selector, -1);
            }
            else
            {
                var source = Cold<IChangeSet<Measurement>>(new ChangeSet<Measurement> { new(ListChangeReason.Add, new Measurement(1, 10)) });
                averages = aggregate ? source.ForAggregation().Avg(selector, -1) : source.Avg(selector, -1);
            }

            using var subscription = averages.RecordValues(out var results);
            await Assert.That(results.RecordedValues).IsEmpty();
            await Assert.That(results.Error).IsSameReferenceAs(expected);
            await Assert.That(acquired).IsEqualTo(1);
            await Assert.That(released).IsEqualTo(1);

            IObservable<TChangeSet> Cold<TChangeSet>(TChangeSet snapshot) => Observable.Create<TChangeSet>(observer =>
            {
                acquired++;
                var disposable = Disposable.Create(() => released++);
                try
                {
                    observer.OnNext(snapshot);
                    return disposable;
                }
                catch
                {
                    disposable.Dispose();
                    throw;
                }
            });
        }

        [Test]
        [Arguments(false, false, false)]
        [Arguments(false, false, true)]
        [Arguments(false, true, false)]
        [Arguments(false, true, true)]
        [Arguments(true, false, false)]
        [Arguments(true, false, true)]
        [Arguments(true, true, false)]
        [Arguments(true, true, true)]
        public async Task NullSelectorIsRejectedBeforeSubscription(bool keyed, bool aggregate, bool nullable)
        {
            using var list = new SourceList<Measurement>();
            using var cache = new SourceCache<Measurement, int>(item => item.Key);
            Func<Measurement, int> selector = null!;
            Func<Measurement, int?> nullableSelector = null!;
            Action create = () =>
            {
                if (keyed)
                {
                    var source = cache.Connect();
                    _ = aggregate
                        ? nullable ? source.ForAggregation().Avg(nullableSelector) : source.ForAggregation().Avg(selector)
                        : nullable ? source.Avg(nullableSelector) : source.Avg(selector);
                }
                else
                {
                    var source = list.Connect();
                    _ = aggregate
                        ? nullable ? source.ForAggregation().Avg(nullableSelector) : source.ForAggregation().Avg(selector)
                        : nullable ? source.Avg(nullableSelector) : source.Avg(selector);
                }
            };

            await Assert.That(create).Throws<ArgumentNullException>();
        }

        [Test]
        [Arguments(false, false)]
        [Arguments(false, true)]
        [Arguments(true, false)]
        [Arguments(true, true)]
        public async Task InitialFallbackTakeOneDisposesExactlyOneSourceSubscription(bool keyed, bool aggregate)
        {
            var acquired = 0;
            var released = 0;
            IObservable<double> averages;
            if (keyed)
            {
                var source = Observable.Create<IChangeSet<Measurement, int>>(_ =>
                {
                    acquired++;
                    return Disposable.Create(() => released++);
                });
                averages = aggregate ? source.ForAggregation().Avg(item => item.Value, -1) : source.Avg(item => item.Value, -1);
            }
            else
            {
                var source = Observable.Create<IChangeSet<Measurement>>(_ =>
                {
                    acquired++;
                    return Disposable.Create(() => released++);
                });
                averages = aggregate ? source.ForAggregation().Avg(item => item.Value, -1) : source.Avg(item => item.Value, -1);
            }

            using var subscription = averages.Take(1).RecordValues(out var results);
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { -1.0 });
            await Assert.That(results.HasCompleted).IsTrue();
            await Assert.That(acquired).IsEqualTo(1);
            await Assert.That(released).IsEqualTo(1);
        }

        [Test]
        public async Task ReentrantInitialFallbackUpdatesAreDeliveredWithoutNestedCallbacks()
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
            var values = new List<double>();
            var depth = 0;
            var maximumDepth = 0;
            using var subscription = source.Avg(value => value, emptyValue: -1).Subscribe(value =>
            {
                maximumDepth = Math.Max(maximumDepth, ++depth);
                values.Add(value);
                if (value == -1)
                {
                    source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 10) });
                }

                depth--;
            });

            await Assert.That(values).IsEquivalentTo(new[] { -1.0, 10.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(maximumDepth).IsEqualTo(1);
            subscription.Dispose();
            await Assert.That(source.HasObservers).IsFalse();
        }

        [Test]
        public async Task DirectNullableNumericOverloadsKeepEveryNullOccurrenceInDenominatorAfterRefresh()
        {
            using var source = new TestSourceList<Measurement>();
            var first = new Measurement(1, 10);
            var second = new Measurement(2, null);
            source.AddRange(new[] { first, second });
            using var intsSubscription = source.Connect().Avg(item => item.Value, -1).RecordValues(out var ints);
            using var longsSubscription = source.Connect().Avg(item => (long?)item.Value, -2L).RecordValues(out var longs);
            using var doublesSubscription = source.Connect().Avg(item => (double?)item.Value, -3D).RecordValues(out var doubles);
            using var decimalsSubscription = source.Connect().Avg(item => (decimal?)item.Value, -4M).RecordValues(out var decimals);
            using var floatsSubscription = source.Connect().Avg(item => (float?)item.Value, -5F).RecordValues(out var floats);
            first.Value = null;
            source.Refresh(0);
            second.Value = 20;
            source.Refresh(1);
            source.Clear();

            await Assert.That(ints.RecordedValues).IsEquivalentTo(new[] { 5D, 0D, 10D, -1D }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(longs.RecordedValues).IsEquivalentTo(new[] { 5D, 0D, 10D, -2D }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(doubles.RecordedValues).IsEquivalentTo(new[] { 5D, 0D, 10D, -3D }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(decimals.RecordedValues).IsEquivalentTo(new[] { 5M, 0M, 10M, -4M }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(floats.RecordedValues).IsEquivalentTo(new[] { 5F, 0F, 10F, -5F }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }

        [Test]
        [Arguments(false, false)]
        [Arguments(false, true)]
        [Arguments(true, false)]
        [Arguments(true, true)]
        public async Task UnchangedFloatingRefreshAndUpdatePreserveLargeOffsetSum(bool keyed, bool aggregate)
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<FloatingMeasurement>>();
            using var cacheSource = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<FloatingMeasurement, int>>();
            var first = new FloatingMeasurement(1, 1e16);
            var second = new FloatingMeasurement(2, -1e16);
            var third = new FloatingMeasurement(3, 1);
            var doubles = keyed
                ? CacheAverage(cacheSource, aggregate)
                : ListAverage(source, aggregate);
            var floats = keyed
                ? CacheAverageFloat(cacheSource, aggregate)
                : ListAverageFloat(source, aggregate);
            using var doublesSubscription = doubles.RecordValues(out var doubleResults);
            using var floatsSubscription = floats.RecordValues(out var floatResults);
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.AddRange, new[] { first, second, third }) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int>
            {
                new(ChangeReason.Add, first.Key, first),
                new(ChangeReason.Add, second.Key, second),
                new(ChangeReason.Add, third.Key, third)
            });
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.Refresh, first, 0) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Refresh, first.Key, first) });
            source.OnNext(new ChangeSet<FloatingMeasurement>
            {
                new(ListChangeReason.Replace, first, ReactiveUI.Primitives.Optional<FloatingMeasurement>.Create(first), 0, 0)
            });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Update, first.Key, first, first) });

            await Assert.That(doubleResults.RecordedValues).IsEquivalentTo(new[] { -1D, 1D / 3, 1D / 3, 1D / 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(floatResults.RecordedValues).IsEquivalentTo(new[] { -1F, 1F / 3, 1F / 3, 1F / 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }

        [Test]
        [Arguments(false, false)]
        [Arguments(false, true)]
        [Arguments(true, false)]
        [Arguments(true, true)]
        public async Task UnchangedInfinityProjectionStaysInfiniteAndEmptyStateRestartsCleanly(bool keyed, bool aggregate)
        {
            using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<FloatingMeasurement>>();
            using var cacheSource = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<FloatingMeasurement, int>>();
            var infinite = new FloatingMeasurement(1, double.PositiveInfinity);
            var doubles = keyed ? CacheAverage(cacheSource, aggregate) : ListAverage(source, aggregate);
            var floats = keyed ? CacheAverageFloat(cacheSource, aggregate) : ListAverageFloat(source, aggregate);
            using var doublesSubscription = doubles.RecordValues(out var doubleResults);
            using var floatsSubscription = floats.RecordValues(out var floatResults);
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.Add, infinite, 0) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Add, infinite.Key, infinite) });
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.Refresh, infinite, 0) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Refresh, infinite.Key, infinite) });
            source.OnNext(new ChangeSet<FloatingMeasurement>
            {
                new(ListChangeReason.Replace, infinite, ReactiveUI.Primitives.Optional<FloatingMeasurement>.Create(infinite), 0, 0)
            });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Update, infinite.Key, infinite, infinite) });
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.Remove, infinite, 0) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Remove, infinite.Key, infinite) });
            var fresh = new FloatingMeasurement(2, 5);
            source.OnNext(new ChangeSet<FloatingMeasurement> { new(ListChangeReason.Add, fresh, 0) });
            cacheSource.OnNext(new ChangeSet<FloatingMeasurement, int> { new(ChangeReason.Add, fresh.Key, fresh) });

            await Assert.That(doubleResults.RecordedValues).IsEquivalentTo(new[] { -1D, double.PositiveInfinity, double.PositiveInfinity, double.PositiveInfinity, -1D, 5D }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(floatResults.RecordedValues).IsEquivalentTo(new[] { -1F, float.PositiveInfinity, float.PositiveInfinity, float.PositiveInfinity, -1F, 5F }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }

        private static IObservable<double> CacheAverage(IObservable<IChangeSet<FloatingMeasurement, int>> source, bool aggregate) =>
            aggregate ? source.ForAggregation().Avg(item => item.Value, -1D) : source.Avg(item => item.Value, -1D);

        private static IObservable<float> CacheAverageFloat(IObservable<IChangeSet<FloatingMeasurement, int>> source, bool aggregate) =>
            aggregate ? source.ForAggregation().Avg(item => (float)item.Value, -1F) : source.Avg(item => (float)item.Value, -1F);

        private static IObservable<double> ListAverage(IObservable<IChangeSet<FloatingMeasurement>> source, bool aggregate) =>
            aggregate ? source.ForAggregation().Avg(item => item.Value, -1D) : source.Avg(item => item.Value, -1D);

        private static IObservable<float> ListAverageFloat(IObservable<IChangeSet<FloatingMeasurement>> source, bool aggregate) =>
            aggregate ? source.ForAggregation().Avg(item => (float)item.Value, -1F) : source.Avg(item => (float)item.Value, -1F);

        private sealed class FloatingMeasurement(int key, double value)
        {
            public int Key { get; } = key;

            public double Value { get; } = value;
        }

        private sealed class Measurement(int key, int? value) : IEquatable<Measurement>
        {
            public int Key { get; } = key;

            public int? Value { get; set; } = value;

            public bool Equals(Measurement? other) => other?.Key == Key;

            public override bool Equals(object? obj) => obj is Measurement other && Equals(other);

            public override int GetHashCode() => Key;
        }
    }
}
