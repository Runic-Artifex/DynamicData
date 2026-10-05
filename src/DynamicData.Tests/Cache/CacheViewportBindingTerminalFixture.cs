#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
using DynamicData.Reactive.Operators;
#else
using DynamicData.Binding;
using DynamicData.Operators;
#endif
using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

public class CacheViewportBindingTerminalFixture
{
    public static IEnumerable<(bool, bool, bool, bool, bool)> Cases()
    {
        foreach (var paged in new[] { false, true })
        foreach (var providedOptions in new[] { false, true })
        foreach (var synchronous in new[] { false, true })
        foreach (var populated in new[] { false, true })
        foreach (var fail in new[] { false, true })
            yield return (paged, providedOptions, synchronous, populated, fail);
    }

    [Test]
    [MethodDataSource(nameof(Cases))]
    public async Task BindingForwardsEmptyAndPopulatedTerminals(bool paged, bool providedOptions, bool synchronous, bool populated, bool fail)
    {
        var expected = new InvalidOperationException("viewport");
        var target = new List<Person>();
        var person = new Person("P", 1);
        var initial = new[] { new Change<Person, string>(ChangeReason.Add, person.Name, person) };
        var comparer = SortExpressionComparer<Person>.Ascending(p => p.Age);
        var completions = 0;
        var errors = new List<Exception>();
        var messages = 0;
        using var pageSignal = new Signal<IChangeSet<Person, string, PageContext<Person>>>();
        using var virtualSignal = new Signal<IChangeSet<Person, string, VirtualContext<Person>>>();
        var page = new ChangeSet<Person, string, PageContext<Person>>(initial, new PageContext<Person>(new PageResponse(1, 1, 1, 1), comparer, new SortAndPageOptions()));
        var virtualized = new ChangeSet<Person, string, VirtualContext<Person>>(initial, new VirtualContext<Person>(new VirtualResponse(0, 1, 1), comparer, new SortAndVirtualizeOptions()));
        IObservable<IChangeSet<Person, string>> bound;
        if (paged)
        {
            var terminal = fail ? Observable.Throw<IChangeSet<Person, string, PageContext<Person>>>(expected) : Observable.Empty<IChangeSet<Person, string, PageContext<Person>>>();
            var source = synchronous ? (populated ? Observable.Return<IChangeSet<Person, string, PageContext<Person>>>(page).Concat(terminal) : terminal) : pageSignal;
            bound = providedOptions ? source.Bind(target, new SortAndBindOptions()) : source.Bind(target);
        }
        else
        {
            var terminal = fail ? Observable.Throw<IChangeSet<Person, string, VirtualContext<Person>>>(expected) : Observable.Empty<IChangeSet<Person, string, VirtualContext<Person>>>();
            var source = synchronous ? (populated ? Observable.Return<IChangeSet<Person, string, VirtualContext<Person>>>(virtualized).Concat(terminal) : terminal) : virtualSignal;
            bound = providedOptions ? source.Bind(target, new SortAndBindOptions()) : source.Bind(target);
        }
        using var subscription = bound.Subscribe(_ => messages++, errors.Add, () => completions++);
        if (!synchronous)
        {
            if (paged)
            {
                if (populated) pageSignal.OnNext(page);
                if (fail) pageSignal.OnError(expected); else pageSignal.OnCompleted();
            }
            else
            {
                if (populated) virtualSignal.OnNext(virtualized);
                if (fail) virtualSignal.OnError(expected); else virtualSignal.OnCompleted();
            }
        }
        await Assert.That(messages).IsEqualTo(populated ? 1 : 0);
        await Assert.That(target.Count).IsEqualTo(populated ? 1 : 0);
        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
        await Assert.That(errors.Count).IsEqualTo(fail ? 1 : 0);
        if (fail) await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }
}
