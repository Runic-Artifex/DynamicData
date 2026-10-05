using System;

#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.AggregationTests;

public partial class AvgFixture
{
    public partial class ForCache
    {
        // Behavioral coverage lives on the inner aggregate overload; ForChangeSet checks forwarding.
        public class ForAggregateChangeSet
        {
            [Test]
            [Arguments(1, 10.0)]
            [Arguments(2, 15.0)]
            [Arguments(3, 20.0)]
            public async Task ItemsAreAdded_AverageReflectsAllItems(int itemCount, double expectedAverage)
            {
                var ages = new[] { 10, 20, 30 };
                using var source = new TestSourceCache<Person, string>(person => person.Name);

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsFalse();
                // Initial-empty behavior is covered separately by the known-defect test.
                var initialValueCount = results.RecordedValues.Count;

                for (var i = 0; i < itemCount; ++i)
                {
                    source.AddOrUpdate(new Person(((char)('A' + i)).ToString(), ages[i]));
                }

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsFalse();
                await Assert.That(results.RecordedValues).HasCount(initialValueCount + itemCount).Because("each edit should produce one average");
                await Assert.That(results.RecordedValues[^1]).IsEqualTo(expectedAverage);
            }

            [Test]
            [Arguments("A", 25.0)]
            [Arguments("B", 20.0)]
            [Arguments("C", 15.0)]
            public async Task ItemIsRemoved_AverageReflectsRemoval(string key, double expectedAverage)
            {
                using var source = CreatePopulatedSource();

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(20.0);

                source.Remove(key);

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsFalse();
                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 20.0, expectedAverage }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            [Test]
            public async Task ItemIsUpdated_AverageReflectsReplacement()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", 10));
                source.AddOrUpdate(new Person("B", 20));

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                source.AddOrUpdate(new Person("B", 50));

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 15.0, 30.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            [Test]
            public async Task MultipleChangesInBatch_SingleAverageIsEmitted()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                var initialValueCount = results.RecordedValues.Count;
                source.Edit(updater =>
                {
                    updater.AddOrUpdate(new Person("A", 10));
                    updater.AddOrUpdate(new Person("B", 20));
                    updater.AddOrUpdate(new Person("C", 30));
                });

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.RecordedValues).HasCount(initialValueCount + 1).Because("one change set should produce one average");
                await Assert.That(results.RecordedValues[^1]).IsEqualTo(20.0);
            }

            [Test]
[Skip("Existing defect: Avg does not emit emptyValue when the source is initially empty. Re-enable once the operator has been rewritten and fixed.")]
            public async Task SourceIsEmpty_ConfiguredEmptyValueIsEmitted()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age, emptyValue: -1)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsFalse();
                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(-1.0);
            }

            [Test]
            public async Task AllItemsAreRemoved_ConfiguredEmptyValueIsEmitted()
            {
                using var source = CreatePopulatedSource();

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age, emptyValue: -1)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                source.Edit(updater => updater.Clear());

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 20.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            // Legacy behavior: nullable projections are coalesced to zero and remain in the denominator.
            [Test]
            public async Task LegacyNullableValuesAreCountedAsZero()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", new int?(10)));
                source.AddOrUpdate(new Person("B", null));
                source.AddOrUpdate(new Person("C", new int?(20)));

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.AgeNullable, emptyValue: -1)
                    .RecordValues(out var results);

                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(10.0).Because("null contributes zero while the item remains in the denominator");
            }

            // Legacy behavior: null coalescing is currently part of the public operator behavior.
            [Test]
            public async Task LegacyAllValuesAreNull_ZeroIsEmittedInsteadOfEmptyValue()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", null));
                source.AddOrUpdate(new Person("B", null));

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.AgeNullable, emptyValue: -1)
                    .RecordValues(out var results);

                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(0.0).Because("a non-empty collection of null projections is not empty");
            }

            [Test]
            public async Task NumericOverloadsProduceExpectedAverages()
            {
                using var source = CreatePopulatedSource();
                var changes = source.Connect().ForAggregation();

                using var intSubscription = changes.Avg(person => person.Age, emptyValue: -1).RecordValues(out var ints);
                using var longSubscription = changes.Avg(person => (long)person.Age, emptyValue: -2L).RecordValues(out var longs);
                using var doubleSubscription = changes.Avg(person => (double)person.Age, emptyValue: -3.0).RecordValues(out var doubles);
                using var decimalSubscription = changes.Avg(person => (decimal)person.Age, emptyValue: -4M).RecordValues(out var decimals);
                using var floatSubscription = changes.Avg(person => (float)person.Age, emptyValue: -5F).RecordValues(out var floats);

                source.Edit(updater => updater.Clear());

                await Assert.That(ints.RecordedValues).IsEquivalentTo(new[] { 20.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(longs.RecordedValues).IsEquivalentTo(new[] { 20.0, -2.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(doubles.RecordedValues).IsEquivalentTo(new[] { 20.0, -3.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(decimals.RecordedValues).IsEquivalentTo(new[] { 20M, -4M }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(floats.RecordedValues).IsEquivalentTo(new[] { 20F, -5F }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            [Test]
            public async Task LegacyNullableNumericOverloadsProduceExpectedAverages()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", new int?(10)));
                source.AddOrUpdate(new Person("B", null));
                var changes = source.Connect().ForAggregation();

                using var intSubscription = changes.Avg(person => person.AgeNullable, emptyValue: -1).RecordValues(out var ints);
                using var longSubscription = changes.Avg(person => (long?)person.AgeNullable, emptyValue: -2L).RecordValues(out var longs);
                using var doubleSubscription = changes.Avg(person => (double?)person.AgeNullable, emptyValue: -3.0).RecordValues(out var doubles);
                using var decimalSubscription = changes.Avg(person => (decimal?)person.AgeNullable, emptyValue: -4M).RecordValues(out var decimals);
                using var floatSubscription = changes.Avg(person => (float?)person.AgeNullable, emptyValue: -5F).RecordValues(out var floats);

                source.Edit(updater => updater.Clear());

                await Assert.That(ints.RecordedValues).IsEquivalentTo(new[] { 5.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(longs.RecordedValues).IsEquivalentTo(new[] { 5.0, -2.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(doubles.RecordedValues).IsEquivalentTo(new[] { 5.0, -3.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(decimals.RecordedValues).IsEquivalentTo(new[] { 5M, -4M }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(floats.RecordedValues).IsEquivalentTo(new[] { 5F, -5F }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            [Test]
            public async Task IntegerAverageCanBeFractional()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", 10));
                source.AddOrUpdate(new Person("B", 11));

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .RecordValues(out var results);

                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(10.5);
            }

            [Test]
[Skip("Existing defect: the legacy aggregate adapter discards Refresh details. Re-enable once the operator has been rewritten and fixed.")]
            public async Task ItemIsRefreshed_AverageReevaluatesMutatedValue()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                var person = new Person("A", 10);
                source.AddOrUpdate(person);

                using var subscription = source.Connect().ForAggregation()
                    .Avg(item => item.Age)
                    .RecordValues(out var results);

                person.Age = 40;
                source.Refresh(person);

                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 10.0, 40.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            [Test]
            [Arguments(StreamCompletionStrategy.Asynchronous)]
            [Arguments(StreamCompletionStrategy.Immediate)]
            public async Task SourceCompletes_CompletionPropagates(StreamCompletionStrategy completionStrategy)
            {
                using var source = CreatePopulatedSource();

                if (completionStrategy is StreamCompletionStrategy.Immediate)
                    source.Complete();

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                if (completionStrategy is StreamCompletionStrategy.Asynchronous)
                    source.Complete();

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsTrue();
                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(20.0);
            }

            [Test]
[Skip("Existing defect: Avg does not emit emptyValue when the source is initially empty. Re-enable once the operator has been rewritten and fixed.")]
            [Arguments(StreamCompletionStrategy.Asynchronous)]
            [Arguments(StreamCompletionStrategy.Immediate)]
            public async Task EmptySourceCompletes_ConfiguredEmptyValueAndCompletionPropagate(StreamCompletionStrategy completionStrategy)
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);

                if (completionStrategy is StreamCompletionStrategy.Immediate)
                    source.Complete();

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age, emptyValue: -1)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                if (completionStrategy is StreamCompletionStrategy.Asynchronous)
                    source.Complete();

                await Assert.That(results.Error).IsNull();
                await Assert.That(results.HasCompleted).IsTrue();
                await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                await Assert.That(results.RecordedValues[0]).IsEqualTo(-1.0);
            }

            [Test]
            [Arguments(StreamCompletionStrategy.Asynchronous)]
            [Arguments(StreamCompletionStrategy.Immediate)]
            public async Task SourceFails_ErrorPropagates(StreamCompletionStrategy completionStrategy)
            {
                using var source = CreatePopulatedSource();
                var error = new Exception("Test error");

                if (completionStrategy is StreamCompletionStrategy.Immediate)
                    source.SetError(error);

                using var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                if (completionStrategy is StreamCompletionStrategy.Asynchronous)
                    source.SetError(error);

                await Assert.That(results.Error).IsSameReferenceAs(error);
                await Assert.That(results.HasCompleted).IsFalse();
                if (completionStrategy is StreamCompletionStrategy.Immediate)
                    await Assert.That(results.RecordedValues).IsEmpty();
                else
                {
                    await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
                    await Assert.That(results.RecordedValues[0]).IsEqualTo(20.0);
                }
            }

            [Test]
            public async Task DisposedSubscriptionReceivesNoFurtherValues()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                var subscription = source.Connect().ForAggregation()
                    .Avg(person => person.Age)
                    .RecordValues(out var results);

                source.AddOrUpdate(new Person("A", 10));
                var valueCountAtDisposal = results.RecordedValues.Count;
                subscription.Dispose();
                source.AddOrUpdate(new Person("B", 20));

                await Assert.That(results.RecordedValues).HasCount(valueCountAtDisposal);
                await Assert.That(results.RecordedValues[^1]).IsEqualTo(10.0);
            }

            [Test]
            public async Task MultipleSubscriptionsMaintainIndependentState()
            {
                using var source = new TestSourceCache<Person, string>(person => person.Name);
                source.AddOrUpdate(new Person("A", 10));
                var averages = source.Connect().ForAggregation().Avg(person => person.Age);

                using var firstSubscription = averages.RecordValues(out var first);
                using var secondSubscription = averages.RecordValues(out var second);
                source.AddOrUpdate(new Person("B", 20));

                await Assert.That(first.RecordedValues).IsEquivalentTo(new[] { 10.0, 15.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
                await Assert.That(second.RecordedValues).IsEquivalentTo(new[] { 10.0, 15.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            private static TestSourceCache<Person, string> CreatePopulatedSource()
            {
                var source = new TestSourceCache<Person, string>(person => person.Name);
                source.Edit(updater =>
                {
                    updater.AddOrUpdate(new Person("A", 10));
                    updater.AddOrUpdate(new Person("B", 20));
                    updater.AddOrUpdate(new Person("C", 30));
                });
                return source;
            }

        }
    }
}
