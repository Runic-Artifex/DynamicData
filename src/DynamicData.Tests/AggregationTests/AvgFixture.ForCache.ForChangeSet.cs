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
        // ForAggregateChangeSet contains the shared behavioral coverage.
        public class ForChangeSet
        {
            [Test]
            public async Task ChangeSetOverload_ForwardsToAggregateOverload()
            {
                using var source = CreatePopulatedSource();

                using var subscription = source.Connect()
                    .Avg(person => person.Age, emptyValue: -1)
                    .RecordValues(out var results);

                source.Edit(updater => updater.Clear());

                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 20.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
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
