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

    private static async Task AssertContains(IObservableCache<Person, string> merged, params (string Name, int Age)[] expected)
    {
        var actual = merged.KeyValues.ToDictionary(kv => kv.Key, kv => kv.Value.Age);
        await Assert.That(actual).IsEquivalentTo(expected.ToDictionary(e => e.Name, e => e.Age));
    }
}
