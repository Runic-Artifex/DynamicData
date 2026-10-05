using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public class BufferInitialFixture
{
    private static readonly ICollection<Person> People = Enumerable.Range(1, 10_000).Select(i => new Person(i.ToString(), i)).ToList();

    [Test]
    public async Task InitialChangesPreserveUpdateAndRemovalOrder()
    {
        var scheduler = new TestScheduler();
        using var source = new SourceCache<Person, string>(person => person.Name);
        using var results = source.Connect().BufferInitial(TimeSpan.FromSeconds(1), scheduler).AsAggregator();
        var first = new Person("same", 1);
        var replacement = new Person("same", 2);
        source.AddOrUpdate(first);
        source.AddOrUpdate(replacement);
        source.Remove(replacement);
        source.AddOrUpdate(first);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(results.Messages.Single().Select(change => change.Reason)).IsEquivalentTo(new[] { ChangeReason.Add, ChangeReason.Update, ChangeReason.Remove, ChangeReason.Add }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Data.Lookup("same").Value).IsSameReferenceAs(first);
    }

    [Test]
    public async Task BufferInitial()
    {
        var scheduler = new TestScheduler();

        using var cache = new SourceCache<Person, string>(i => i.Name);
        using var aggregator = cache.Connect().BufferInitial(TimeSpan.FromSeconds(1), scheduler).AsAggregator();
        foreach (var item in People)
        {
            cache.AddOrUpdate(item);
        }

        await Assert.That(aggregator.Data.Count).IsEqualTo(0);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(0);

        scheduler.Start();

        await Assert.That(aggregator.Data.Count).IsEqualTo(10_000);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(1);

        cache.AddOrUpdate(new Person("_New", 1));

        await Assert.That(aggregator.Data.Count).IsEqualTo(10_001);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(2);
    }
}
