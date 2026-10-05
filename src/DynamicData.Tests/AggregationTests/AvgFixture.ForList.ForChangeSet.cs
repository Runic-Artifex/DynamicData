#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public partial class AvgFixture
{
    public partial class ForList
    {
        // ForAggregateChangeSet contains the shared behavioral coverage.
        public class ForChangeSet
        {
            [Test]
            public async Task ChangeSetOverload_ForwardsToAggregateOverload()
            {
                using var source = CreatePopulatedSource();

                using var subscription = source.Connect()
                    .Avg(value => value, emptyValue: -1)
                    .RecordValues(out var results);

                source.Clear();

                await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 20.0, -1.0 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
            }

            private static TestSourceList<int> CreatePopulatedSource()
            {
                var source = new TestSourceList<int>();
                source.AddRange(new[] { 10, 20, 30 });
                return source;
            }
        }
    }
}
