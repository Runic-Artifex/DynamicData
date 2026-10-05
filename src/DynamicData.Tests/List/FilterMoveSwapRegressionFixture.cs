#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.List;

public sealed class FilterMoveSwapRegressionFixture
{
    public enum Mode
    {
        Static,
        DynamicDiff,
        DynamicReset
    }

    [Test]
    [Arguments(Mode.Static)]
    [Arguments(Mode.DynamicDiff)]
    [Arguments(Mode.DynamicReset)]
    public async Task ObservableCollectionMove_PreservesIndexedOrder(Mode mode)
    {
        var source = new ObservableCollection<string>();
        using var subscription = Apply(source.ToObservableChangeSet(), mode, static _ => true)
            .ValidateChangeSets().Bind(out var bound).RecordListItems(out var results);
        source.Add("c");
        source.Add("a");
        source.Add("b");
        source.Move(0, 2);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { "a", "b", "c" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(bound).IsEquivalentTo(source, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(Mode.Static)]
    [Arguments(Mode.DynamicDiff)]
    [Arguments(Mode.DynamicReset)]
    public async Task ObservableCollectionSwap_PreservesBothOccurrencesDuringReplacement(Mode mode)
    {
        var source = new ObservableCollection<string>();
        using var subscription = Apply(source.ToObservableChangeSet(), mode, static _ => true)
            .ValidateChangeSets().Bind(out var bound).RecordListItems(out var results);
        source.Add("c");
        source.Add("b");
        source.Add("a");
        var temp = source[0];
        source[0] = source[2];
        await Assert.That(results.RecordedItems).IsEquivalentTo(source, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source[2] = temp;

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { "a", "b", "c" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(bound).IsEquivalentTo(source, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(Mode.Static)]
    [Arguments(Mode.DynamicDiff)]
    [Arguments(Mode.DynamicReset)]
    public async Task MatchingAndExcludedMovesAndIndexedAdds_ReplayInSourceOrder(Mode mode)
    {
        var source = new ObservableCollection<int> { 1, 2, 3, 4, 5, 6 };
        using var subscription = Apply(source.ToObservableChangeSet(), mode, static value => value % 2 != 0)
            .ValidateChangeSets().RecordListItems(out var results);

        for (var i = 0; i < 8; i++)
        {
            source.Move(i % source.Count, source.Count - 1 - (i % source.Count));
            await Assert.That(results.Error).IsNull();
            await Assert.That(results.RecordedItems).IsEquivalentTo(source.Where(static value => value % 2 != 0), TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }

        source.Insert(0, 7);
        source.Insert(2, 8);
        source[3] = 9;
        source.RemoveAt(1);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(source.Where(static value => value % 2 != 0), TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(ListFilterPolicy.CalculateDiff)]
    [Arguments(ListFilterPolicy.ClearAndReplace)]
    public async Task DynamicPredicateChanges_PreserveMembershipAndPolicyOrder(ListFilterPolicy policy)
    {
        using var source = new SourceList<int>();
        using var predicates = new ReactiveUI.Primitives.Signals.Signal<Func<int, bool>>();
        using var subscription = source.Connect().Filter(predicates, policy).ValidateChangeSets().RecordListItems(out var results);
        source.AddRange(new[] { 1, 2, 3, 4, 5 });
        predicates.OnNext(static value => value % 2 != 0);
        predicates.OnNext(static _ => true);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.OrderBy(static value => value)).IsEquivalentTo(source.Items.OrderBy(static value => value), TUnit.Assertions.Enums.CollectionOrdering.Matching);
        var expected = policy == ListFilterPolicy.ClearAndReplace ? new[] { 1, 2, 3, 4, 5 } : new[] { 1, 3, 5, 2, 4 };
        await Assert.That(results.RecordedItems).IsEquivalentTo(expected, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(Mode.Static)]
    [Arguments(Mode.DynamicDiff)]
    [Arguments(Mode.DynamicReset)]
    public async Task RandomIndexedChangesWithDuplicateValues_MatchOrderedReference(Mode mode)
    {
        var random = new Random(0x1044);
        var source = new ObservableCollection<int>();
        using var subscription = Apply(source.ToObservableChangeSet(), mode, static value => value % 2 != 0)
            .ValidateChangeSets().RecordListItems(out var results);
        for (var iteration = 0; iteration < 250; iteration++)
        {
            var operation = source.Count == 0 ? 0 : random.Next(4);
            switch (operation)
            {
                case 0:
                    source.Insert(random.Next(source.Count + 1), random.Next(10));
                    break;
                case 1:
                    source.RemoveAt(random.Next(source.Count));
                    break;
                case 2:
                    source[random.Next(source.Count)] = random.Next(10);
                    break;
                case 3:
                    source.Move(random.Next(source.Count), random.Next(source.Count));
                    break;
            }

            await Assert.That(results.Error).IsNull();
            await Assert.That(results.RecordedItems).IsEquivalentTo(source.Where(static value => value % 2 != 0), TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }
    }

    [Test]
    [Arguments(ListFilterPolicy.CalculateDiff)]
    [Arguments(ListFilterPolicy.ClearAndReplace)]
    public async Task RequeryThenReplaceRemoveAndRefresh_PreserveEqualReferenceOccurrences(ListFilterPolicy policy)
    {
        using var source = new SourceList<EqualRow>();
        using var predicates = new ReactiveUI.Primitives.Signals.Signal<Func<EqualRow, bool>>();
        using var refreshes = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualRow>>();
        using var subscription = source.Connect().Merge(refreshes).Filter(predicates, policy).ValidateChangeSets().RecordListItems(out var results);
        var first = new EqualRow(1, true);
        var second = new EqualRow(1, false);
        var last = new EqualRow(2, true);
        predicates.OnNext(static row => row.Included);
        source.AddRange(new[] { first, second, last });
        predicates.OnNext(static _ => true);
        var replacement = new EqualRow(1, true);
        source.ReplaceAt(1, replacement);
        source.RemoveAt(0);
        var expected = policy == ListFilterPolicy.ClearAndReplace ? new[] { replacement, last } : new[] { last, replacement };

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Count).IsEqualTo(2);
        for (var i = 0; i < expected.Length; i++)
            await Assert.That(results.RecordedItems[i]).IsSameReferenceAs(expected[i]);

        predicates.OnNext(static row => row.Included);
        replacement.Included = false;
        refreshes.OnNext(new ChangeSet<EqualRow> { new(ListChangeReason.Refresh, replacement, 0) });
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Count).IsEqualTo(1);
        await Assert.That(results.RecordedItems[0]).IsSameReferenceAs(last);
        predicates.OnNext(static _ => false);
        await Assert.That(results.RecordedItems).IsEmpty();
    }

    private sealed class EqualRow(int id, bool included)
    {
        public int Id { get; } = id;

        public bool Included { get; set; } = included;

        public override bool Equals(object? obj) => obj is EqualRow other && Id == other.Id;

        public override int GetHashCode() => Id;
    }

    [Test]
    [Arguments(ListFilterPolicy.CalculateDiff)]
    [Arguments(ListFilterPolicy.ClearAndReplace)]
    public async Task SourceNoOpMoveAfterRequery_DoesNotRearrangeHistoricalOrder(ListFilterPolicy policy)
    {
        var source = new ObservableCollection<int> { 1, 2, 3, 4, 5 };
        using var predicates = new ReactiveUI.Primitives.Signals.Signal<Func<int, bool>>();
        using var subscription = source.ToObservableChangeSet().Filter(predicates, policy).ValidateChangeSets().RecordListItems(out var results);
        predicates.OnNext(static value => value % 2 != 0);
        predicates.OnNext(static _ => true);
        var previous = results.RecordedItems.ToArray();
        var messages = results.RecordedChangeSets.Count;
        source.Move(2, 2);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(previous, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(messages);

        predicates.OnNext(static value => value != 3);
        previous = results.RecordedItems.ToArray();
        messages = results.RecordedChangeSets.Count;
        source.Move(3, 2); // Cross an excluded row without changing the matching source rank.
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(previous, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(messages);
    }

    private static IObservable<IChangeSet<T>> Apply<T>(IObservable<IChangeSet<T>> source, Mode mode, Func<T, bool> predicate)
        where T : notnull => mode == Mode.Static
            ? source.Filter(predicate)
            : source.Filter(Observable.Return(predicate), mode == Mode.DynamicDiff ? ListFilterPolicy.CalculateDiff : ListFilterPolicy.ClearAndReplace);
}
