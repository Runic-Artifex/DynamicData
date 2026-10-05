using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

/// <summary>Live children remain owned after the parent completes; errors terminate all ownership.</summary>
public class CacheChildTerminalContractFixture
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task MergeWaitsForChildAfterParentAndFailsFast(bool items, bool fail)
    {
        using var parent = new Signal<IChangeSet<Person, string>>();
        using var child = new Signal<int>();
        var disposals = 0;
        var trackedChild = child.Finally(() => disposals++);
        var values = new List<int>();
        var completions = 0;
        Exception? error = null;
        var stream = items ? parent.MergeManyItems((_, _) => trackedChild).Select(pair => pair.Value) : parent.MergeMany((_, _) => trackedChild);
        using var subscription = stream.Subscribe(values.Add, ex => error = ex, () => completions++);
        var person = new Person("P", 1);
        parent.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        parent.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(disposals).IsEqualTo(0);
        child.OnNext(42);
        var expected = new InvalidOperationException("child");
        if (fail) child.OnError(expected); else child.OnCompleted();
        await Assert.That(values).IsEquivalentTo(new[] { 42 });
        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
        await Assert.That(error).IsSameReferenceAs(fail ? expected : null);
        await Assert.That(disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RemovedOrReplacedChildIsDisposedAndCannotPublish(bool replace)
    {
        using var parent = new SourceCache<Person, string>(p => p.Name);
        using var oldChild = new Signal<int>();
        using var newChild = new Signal<int>();
        var oldPerson = new Person("P", 1);
        var newPerson = new Person("P", 2);
        var oldDisposals = 0;
        var newDisposals = 0;
        var values = new List<int>();
        var completions = 0;
        using var subscription = parent.Connect().MergeMany(p => ReferenceEquals(p, oldPerson)
            ? oldChild.Finally(() => oldDisposals++) : newChild.Finally(() => newDisposals++))
            .Subscribe(values.Add, static _ => { }, () => completions++);
        parent.AddOrUpdate(oldPerson);
        oldChild.OnNext(1);
        if (replace) parent.AddOrUpdate(newPerson); else parent.Remove("P");
        oldChild.OnNext(2);
        await Assert.That(oldDisposals).IsEqualTo(1);
        subscription.Dispose();
        newChild.OnNext(3);
        parent.AddOrUpdate(newPerson);
        await Assert.That(values).IsEquivalentTo(new[] { 1 });
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(newDisposals).IsEqualTo(replace ? 1 : 0);
    }

    [Test]
    public async Task ReentrantDisposalDuringChildValueDoesNotComplete()
    {
        using var parent = new SourceCache<Person, string>(p => p.Name);
        using var child = new Signal<int>();
        var completions = 0;
        var values = 0;
        var childDisposals = 0;
        IDisposable? subscription = null;
        subscription = parent.Connect().MergeMany(_ => child.Finally(() => childDisposals++))
            .Subscribe(_ => { values++; subscription!.Dispose(); }, static _ => { }, () => completions++);
        parent.AddOrUpdate(new Person("P", 1));
        child.OnNext(1);
        child.OnNext(2);
        parent.Dispose();
        await Assert.That(values).IsEqualTo(1);
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(childDisposals).IsEqualTo(1);
    }

    [Test]
    public async Task ChildErrorDisposesSiblingAndParent()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var left = new Signal<int>();
        using var right = new Signal<int>();
        var parentDisposals = 0;
        var childDisposals = 0;
        Exception? error = null;
        using var subscription = source.Finally(() => parentDisposals++).MergeMany(p => (p.Age == 1 ? left : right).Finally(() => childDisposals++))
            .Subscribe(static _ => { }, ex => error = ex);
        var first = new Person("one", 1);
        var second = new Person("two", 2);
        source.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, first.Name, first), new Change<Person, string>(ChangeReason.Add, second.Name, second)]));
        var expected = new InvalidOperationException("fail fast");
        left.OnError(expected);
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(parentDisposals).IsEqualTo(1);
        await Assert.That(childDisposals).IsEqualTo(2);
    }

    [Test]
    public async Task ListChildReplacementRemovesBeforeSynchronousSnapshot()
    {
        using var owners = new SourceCache<AnimalOwner, Guid>(o => o.Id);
        var owner = new AnimalOwner("Owner");
        owner.Animals.AddRange([new Animal("A1", "Type", AnimalFamily.Mammal), new Animal("A2", "Type", AnimalFamily.Mammal)]);
        owners.AddOrUpdate(owner);
        using var subscription = owners.Connect().MergeManyChangeSets(o => o.Animals.Connect())
            .Transform(a => new Person(a.Name, a.Name.Length)).AddKey(static p => p.Name)
            .ValidateChangeSets(static p => p.Name).RecordCacheItems(out var result);
        owners.AddOrUpdate(owner);
        await Assert.That(result.Error).IsNull();
        await Assert.That(result.RecordedItemsByKey.Count).IsEqualTo(2);
    }
}
