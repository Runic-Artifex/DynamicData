namespace DynamicData.Tests.List;

public sealed class TransformOccurrenceFixture
{
    [Test]
    public async Task IndexlessValueRemovalsAndAddsInOneBatchPreserveOrderAndMultiplicity()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var results = source.Transform(value => value).AsAggregator();

        source.OnNext(new ChangeSet<int>
        {
            new(ListChangeReason.AddRange, new[] { 1, 2, 1, 3, 2 })
        });
        source.OnNext(new ChangeSet<int>
        {
            new(ListChangeReason.Remove, 1),
            new(ListChangeReason.Add, 4, 1),
            new(ListChangeReason.RemoveRange, new[] { 2, 1 }),
            new(ListChangeReason.AddRange, new[] { 5, 5 })
        });
        source.OnCompleted();

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 4, 3, 2, 5, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(3);
        await Assert.That(results.Messages[1].Adds).IsEqualTo(3);
        await Assert.That(results.Exception).IsNull();
        await Assert.That(results.IsCompleted).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IndexlessRemovalPrefersReferenceIdentityOverEqualSources(bool range)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.Transform(item => item.Name).AsAggregator();
        var first = new EqualItem(1, "first");
        var second = new EqualItem(1, "second");
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { first, second })
        });
        source.OnNext(new ChangeSet<EqualItem>
        {
            range ? new(ListChangeReason.RemoveRange, new[] { second }) : new(ListChangeReason.Remove, second)
        });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "first" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IndexlessRemovalFallsBackToSourceEqualityAndIgnoresMissingValues(bool range)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.Transform(item => item.Name).AsAggregator();
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { new EqualItem(1, "first"), new EqualItem(1, "second"), new EqualItem(2, "other") })
        });
        var equal = new EqualItem(1, "equal removal");
        var missing = new EqualItem(3, "missing");
        source.OnNext(range
            ? new ChangeSet<EqualItem> { new(ListChangeReason.RemoveRange, new[] { equal, missing }) }
            : new ChangeSet<EqualItem> { new(ListChangeReason.Remove, equal), new(ListChangeReason.Remove, missing) });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "second", "other" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task IndexlessRangeConsumesExactlyOneOccurrencePerRepeatedReference()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.Transform((EqualItem item, int index) => $"{item.Name}:{index}").AsAggregator();
        var repeated = new EqualItem(1, "repeated");
        var other = new EqualItem(2, "other");
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { repeated, other, repeated, repeated })
        });
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.RemoveRange, new[] { repeated, repeated })
        });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "other:1", "repeated:3" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(2);
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.RemoveRange, new[] { repeated, repeated })
        });
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "other:1" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[2].Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task IndexlessSourceRemovalsEmitIndexesThatReplayThroughAnotherTransform()
    {
        using var source = new SourceList<int>();
        var transformed = source.Connect().RemoveIndex().Transform(value => $"item:{value}").Publish();
        using var first = transformed.AsAggregator();
        using var replay = transformed.Transform(value => $"replayed:{value}").AsAggregator();
        using var connection = transformed.Connect();
        source.AddRange(new[] { 1, 2, 1, 3, 2 });
        source.RemoveAt(2);
        source.RemoveRange(2, 2);

        await Assert.That(first.Data.Items).IsEquivalentTo(new[] { "item:1", "item:2" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(replay.Data.Items).IsEquivalentTo(new[] { "replayed:item:1", "replayed:item:2" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(first.Messages[1].Single().Item.CurrentIndex).IsEqualTo(0);
        await Assert.That(first.Messages[2].Removes).IsEqualTo(2);
        await Assert.That(first.Exception).IsNull();
        await Assert.That(replay.Exception).IsNull();
    }

    private sealed class EqualItem(int key, string name) : IEquatable<EqualItem>
    {
        public string Name { get; } = name;

        private int Key { get; } = key;

        public bool Equals(EqualItem? other) => other?.Key == Key;

        public override bool Equals(object? obj) => obj is EqualItem other && Equals(other);

        public override int GetHashCode() => Key;
    }
}
