using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

public class CacheBatchTerminalContractFixture
{
    [Test]
    [Arguments("sourceComplete")]
    [Arguments("sourceError")]
    [Arguments("pauseError")]
    [Arguments("timerError")]
    [Arguments("dispose")]
    public async Task BufferedChangesFlushOnlyOnCompletion(string terminal)
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var pause = new Signal<bool>();
        using var timer = new Signal<Unit>();
        var values = new List<IChangeSet<Person, string>>();
        var completions = 0;
        var disposals = 0;
        Exception? error = null;
        var expected = new InvalidOperationException(terminal);
        using var subscription = source.Finally(() => disposals++)
            .BatchIf(pause, initialPauseState: true, timer: timer)
            .Subscribe(values.Add, ex => error = ex, () => completions++);
        var person = new Person("P", 1);
        source.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        switch (terminal)
        {
            case "sourceComplete": source.OnCompleted(); break;
            case "sourceError": source.OnError(expected); break;
            case "pauseError": pause.OnError(expected); break;
            case "timerError": timer.OnError(expected); break;
            case "dispose": subscription.Dispose(); break;
        }
        pause.OnNext(false);
        timer.OnNext(Unit.Default);
        var complete = terminal == "sourceComplete";
        await Assert.That(values.Count).IsEqualTo(complete ? 1 : 0);
        if (complete) await Assert.That(values[0].Single().Current).IsSameReferenceAs(person);
        await Assert.That(completions).IsEqualTo(complete ? 1 : 0);
        await Assert.That(error).IsSameReferenceAs(complete || terminal == "dispose" ? null : expected);
        await Assert.That(disposals).IsEqualTo(1);
    }
}
