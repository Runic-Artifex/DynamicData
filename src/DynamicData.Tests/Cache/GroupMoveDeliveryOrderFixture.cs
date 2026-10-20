// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

/// <summary>
/// When an item moves from one group to another, the old group must publish its removal before the
/// new group publishes the addition. Otherwise flattening the groups again (<c>MergeMany</c> over the
/// group caches, or <c>TransformMany</c> over immutable groups) applies the stale removal last and
/// loses the item (fork issue #29).
/// </summary>
public sealed class GroupMoveDeliveryOrderFixture
{
    private static readonly Func<Person, int> ByAge = p => p.Age;

    [Test]
    public async Task RegrouperMovesIntoExistingGroup()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceCache<Person, string>(p => p.Name);
        var a = new Person("a", 1);
        source.AddOrUpdate([a, new Person("b", 2)]);

        using var merged = source.Connect().Group(ByAge, regrouper).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        a.Age = 2;
        regrouper.OnNext(default(Unit));

        await AssertContains(merged, ("a", 2), ("b", 2));
    }

    [Test]
    public async Task RegrouperMovesOutOfRemainingGroup()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceCache<Person, string>(p => p.Name);
        var a = new Person("a", 1);
        source.AddOrUpdate([a, new Person("b", 2), new Person("c", 1)]);

        using var merged = source.Connect().Group(ByAge, regrouper).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        a.Age = 2;
        regrouper.OnNext(default(Unit));

        await AssertContains(merged, ("a", 2), ("b", 2), ("c", 1));
    }

    [Test]
    public async Task RegrouperSwapsItemsBetweenGroups()
    {
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceCache<Person, string>(p => p.Name);
        var a = new Person("a", 1);
        var b = new Person("b", 2);
        source.AddOrUpdate([a, b, new Person("c", 1), new Person("d", 2)]);

        using var merged = source.Connect().Group(ByAge, regrouper).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        a.Age = 2;
        b.Age = 1;
        regrouper.OnNext(default(Unit));

        await AssertContains(merged, ("a", 2), ("b", 1), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task UpdateMovesIntoExistingGroup()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2)]);

        using var merged = source.Connect().Group(ByAge).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate(new Person("a", 2));

        await AssertContains(merged, ("a", 2), ("b", 2));
    }

    [Test]
    public async Task UpdatesSwapItemsBetweenGroups()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1), new Person("d", 2)]);

        using var merged = source.Connect().Group(ByAge).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate([new Person("a", 2), new Person("b", 1)]);

        await AssertContains(merged, ("a", 2), ("b", 1), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task DynamicSelectorMovesIntoExistingGroup()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        selector.OnNext((p, k) => k == "a" ? 2 : p.Age);

        await AssertContains(merged, ("a", 1), ("b", 2), ("c", 1));
    }

    [Test]
    public async Task DynamicSelectorEmptiesGroup()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        selector.OnNext((_, _) => 2);

        await AssertContains(merged, ("a", 1), ("b", 2));
    }

    [Test]
    public async Task DynamicSelectorSwapsItemsBetweenGroups()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1), new Person("d", 2)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        selector.OnNext((p, k) => k switch { "a" => 2, "b" => 1, _ => p.Age });

        await AssertContains(merged, ("a", 1), ("b", 2), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task DynamicRegrouperMovesIntoExistingGroup()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var regrouper = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var source = new SourceCache<Person, string>(p => p.Name);
        var a = new Person("a", 1);
        source.AddOrUpdate([a, new Person("b", 2), new Person("c", 1)]);

        using var merged = source.Connect().Group(selector, regrouper).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        a.Age = 2;
        regrouper.OnNext(default(Unit));

        await AssertContains(merged, ("a", 2), ("b", 2), ("c", 1));
    }

    [Test]
    public async Task DynamicSelectorBatchMovesIntoGroupItAlsoUpdates()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate([new Person("b", 2), new Person("a", 2)]);

        await AssertContains(merged, ("a", 2), ("b", 2), ("c", 1));
    }

    [Test]
    public async Task DynamicSelectorBatchSwapsItemsBetweenGroups()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1), new Person("d", 2)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate([new Person("a", 2), new Person("b", 1)]);

        await AssertContains(merged, ("a", 2), ("b", 1), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task DynamicSelectorBatchRemovesLastItemOfGroup()
    {
        using var selector = new ReactiveUI.Primitives.Signals.StateSignal<Func<Person, string, int>>((p, _) => p.Age);
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2)]);

        using var merged = source.Connect().Group(selector).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.Edit(updater =>
        {
            updater.RemoveKey("a");
            updater.AddOrUpdate(new Person("b", 3));
        });

        await AssertContains(merged, ("b", 3));
    }

    [Test]
    public async Task ObservableGroupKeyBatchMovesIntoGroupItAlsoAddsTo()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1)]);

        using var merged = source.Connect().GroupOnObservable(p => Observable.Return(p.Age)).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate([new Person("d", 2), new Person("a", 2)]);

        await AssertContains(merged, ("a", 2), ("b", 2), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task ObservableGroupKeyBatchSwapsItemsBetweenGroups()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        source.AddOrUpdate([new Person("a", 1), new Person("b", 2), new Person("c", 1), new Person("d", 2)]);

        using var merged = source.Connect().GroupOnObservable(p => Observable.Return(p.Age)).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.AddOrUpdate([new Person("a", 2), new Person("b", 1)]);

        await AssertContains(merged, ("a", 2), ("b", 1), ("c", 1), ("d", 2));
    }

    [Test]
    public async Task ObservableGroupKeyBatchReaddsRemovedItemToEarlierGroup()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Person, string>>();
        var a = new Person("a", 1);
        var b = new Person("b", 2);
        var d = new Person("d", 2);

        using var merged = source.GroupOnObservable(p => Observable.Return(p.Age)).MergeMany(g => g.Cache.Connect()).AsObservableCache();

        source.OnNext(new ChangeSet<Person, string> { new(ChangeReason.Add, "a", a), new(ChangeReason.Add, "b", b) });
        var moved = new Person("a", 2);
        source.OnNext(new ChangeSet<Person, string> { new(ChangeReason.Add, "d", d), new(ChangeReason.Remove, "a", a), new(ChangeReason.Add, "a", moved) });

        await AssertContains(merged, ("a", 2), ("b", 2), ("d", 2));
    }

    private static async Task AssertContains(IObservableCache<Person, string> merged, params (string Name, int Age)[] expected)
    {
        var actual = merged.KeyValues.ToDictionary(kv => kv.Key, kv => kv.Value.Age);
        await Assert.That(actual).IsEquivalentTo(expected.ToDictionary(e => e.Name, e => e.Age));
    }
}
