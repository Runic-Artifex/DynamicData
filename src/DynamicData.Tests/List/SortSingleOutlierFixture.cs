namespace DynamicData.Tests.List;

public sealed class SortSingleOutlierFixture
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task SingleMutatedOutlier_ReplaysAsOneMove(bool moveTowardStart, bool changeComparer)
    {
        using var source = new SourceList<Row>();
        using var resort = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var comparers = new ReactiveUI.Primitives.Signals.Signal<IComparer<Row>>();
        var comparer = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        using var subscription = source.Connect().Sort(comparer, resort: resort, comparerChanged: comparers, resetThreshold: int.MaxValue)
            .ValidateChangeSets().RecordListItems(out var results);
        var rows = Enumerable.Range(0, 100).Select(static value => new Row(value, value)).ToArray();
        source.AddRange(rows);
        rows[moveTowardStart ? ^1 : 0].Key = moveTowardStart ? -1 : 100;
        var expected = results.RecordedItems.OrderBy(static row => row, comparer).ToArray();

        if (changeComparer)
            comparers.OnNext(comparer);
        else
            resort.OnNext(Unit.Default);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets[^1].Moves).IsEqualTo(1);
        await Assert.That(results.RecordedChangeSets[^1].Count).IsEqualTo(1);
    }

    [Test]
    public async Task SingleRefresh_AlreadyReplaysAsOneMove()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Row>>();
        var comparer = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        using var subscription = source.Sort(comparer, resetThreshold: int.MaxValue).ValidateChangeSets().RecordListItems(out var results);
        var rows = Enumerable.Range(0, 100).Select(static value => new Row(value, value)).ToArray();
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.AddRange, rows, 0) });
        rows[0].Key = 100;
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Refresh, rows[0], 0) });

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(rows.Skip(1).Append(rows[0]), TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets[^1].Moves).IsEqualTo(1);
        await Assert.That(results.RecordedChangeSets[^1].Refreshes).IsEqualTo(1);
    }

    [Test]
    public async Task RandomMutationsAndComparerChanges_StableReplayMatchesReferenceOrder()
    {
        var random = new Random(0x1066);
        using var source = new SourceList<Row>();
        using var resort = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var comparers = new ReactiveUI.Primitives.Signals.Signal<IComparer<Row>>();
        var ascending = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        var descending = Comparer<Row>.Create(static (left, right) => right.Key.CompareTo(left.Key));
        var comparer = ascending;
        using var subscription = source.Connect().Sort(comparer, resort: resort, comparerChanged: comparers, resetThreshold: int.MaxValue)
            .ValidateChangeSets().RecordListItems(out var results);
        var rows = Enumerable.Range(0, 64).Select(value => new Row(value, random.Next(8))).ToArray();
        source.AddRange(rows);

        for (var iteration = 0; iteration < 200; iteration++)
        {
            var previousOrder = results.RecordedItems.ToArray();
            var mutationCount = iteration % 3 == 0 ? 5 : 1;
            for (var mutation = 0; mutation < mutationCount; mutation++)
                rows[random.Next(rows.Length)].Key = random.Next(12);

            if (iteration % 7 == 0)
                comparer = ReferenceEquals(comparer, ascending) ? descending : ascending;
            var expected = previousOrder.OrderBy(static row => row, comparer).ToArray();
            if (iteration % 7 == 0)
                comparers.OnNext(comparer);
            else
                resort.OnNext(Unit.Default);

            await Assert.That(results.Error).IsNull();
            await Assert.That(results.RecordedItems).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }
    }

    [Test]
    public async Task DuplicateReferencesAndValues_ReplayWithoutLosingOccurrences()
    {
        var comparer = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        var duplicate = new Row(0, 0);
        var rows = new[] { duplicate, duplicate, new Row(1, 1), new Row(2, 2), new Row(3, 2) };
        using var source = new SourceList<Row>();
        using var resort = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var subscription = source.Connect().Sort(comparer, resort: resort, resetThreshold: int.MaxValue)
            .ValidateChangeSets().RecordListItems(out var results);
        source.AddRange(rows);
        duplicate.Key = 3;
        resort.OnNext(Unit.Default);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(rows.OrderBy(static row => row, comparer), TUnit.Assertions.Enums.CollectionOrdering.Matching);

        using var values = new SourceList<int>();
        using var changes = new ReactiveUI.Primitives.Signals.Signal<IComparer<int>>();
        using var valueSubscription = values.Connect().Sort(Comparer<int>.Default, comparerChanged: changes, resetThreshold: int.MaxValue)
            .ValidateChangeSets().RecordListItems(out var valueResults);
        values.AddRange(new[] { 2, 1, 1, 3, 2 });
        changes.OnNext(Comparer<int>.Create(static (left, right) => right.CompareTo(left)));
        await Assert.That(valueResults.Error).IsNull();
        await Assert.That(valueResults.RecordedItems).IsEquivalentTo(new[] { 3, 2, 2, 1, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    private sealed class Row(int id, int key)
    {
        public int Id { get; } = id;

        public int Key { get; set; } = key;
    }

    [Test]
    public async Task DistinctEqualReferences_GeneralReorderPreservesExactOccurrences()
    {
        using var source = new SourceList<EqualRow>();
        using var comparers = new ReactiveUI.Primitives.Signals.Signal<IComparer<EqualRow>>();
        var ascending = Comparer<EqualRow>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        var descending = Comparer<EqualRow>.Create(static (left, right) => right.Key.CompareTo(left.Key));
        using var subscription = source.Connect().Sort(ascending, comparerChanged: comparers, resetThreshold: int.MaxValue)
            .ValidateChangeSets().RecordListItems(out var results);
        var rows = Enumerable.Range(0, 6).Select(static key => new EqualRow(key)).ToArray();
        source.AddRange(rows);
        comparers.OnNext(descending);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Count).IsEqualTo(rows.Length);
        for (var i = 0; i < rows.Length; i++)
            await Assert.That(results.RecordedItems[i]).IsSameReferenceAs(rows[rows.Length - i - 1]);
    }

    private sealed class EqualRow(int key)
    {
        public int Key { get; } = key;

        public override bool Equals(object? obj) => obj is EqualRow;

        public override int GetHashCode() => 0;
    }
}
