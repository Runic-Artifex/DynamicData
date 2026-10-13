#if REACTIVE_TESTS
using DynamicData.Reactive.Experimental;
#else
using DynamicData.Experimental;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public class WatcherFixture : IDisposable
{
    private readonly IDisposable _cleanUp;

    private readonly ChangeSetAggregator<SelfObservingPerson, string> _results;

    private readonly TestScheduler _scheduler = new();

    private readonly ISourceCache<Person, string> _source;

    private readonly IWatcher<Person, string> _watcher;

    public WatcherFixture()
    {
        _scheduler = new TestScheduler();
        _source = new SourceCache<Person, string>(p => p.Key);
        _watcher = _source.Connect().AsWatcher(_scheduler);

        _results = new ChangeSetAggregator<SelfObservingPerson, string>(_source.Connect().Transform(p => new SelfObservingPerson(_watcher.Watch(p.Key).Select(w => w.Current))).DisposeMany());

        _cleanUp = Disposable.Create(
            () =>
            {
                _results.Dispose();
                _source.Dispose();
                _watcher.Dispose();
            });
    }

    [Test]
    public async Task AddNew()
    {
        var person = new Person("Adult1", 50);
        _source.AddOrUpdate(person);

        await Assert.That(_results.Data.Count).IsEqualTo(1).Because("Should be 1 item in the cache");

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(50).Ticks);
        var result = _results.Data.Items[0];
        await Assert.That(result.UpdateCount).IsEqualTo(1).Because("Person should have received 1 update");
        await Assert.That(result.Completed).IsFalse().Because("Person should have received 1 update");
    }

    public void Dispose()
    {
        _cleanUp.Dispose();
        _results.Dispose();
        _source.Dispose();
        _watcher.Dispose();
    }

    [Test]
    public async Task Remove()
    {
        var person = new Person("Adult1", 50);
        _source.AddOrUpdate(person);

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);
        _source.Remove(person.Key);

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(11).Ticks);
        await Assert.That(_results.Messages.Count).IsEqualTo(2).Because("Should be 1 updates");
        await Assert.That(_results.Data.Count).IsEqualTo(0).Because("Should be 0 item in the cache");

        var secondResult = _results.Messages[1].First();
        await Assert.That(secondResult.Current.UpdateCount).IsEqualTo(1).Because("Second Person should have received 1 update");
        await Assert.That(secondResult.Current.Completed).IsTrue().Because("Second person  should have received 1 update");
    }

    [Test]
    public async Task Update()
    {
        var first = new Person("Adult1", 50);
        var second = new Person("Adult1", 51);
        _source.AddOrUpdate(first);

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);
        _source.AddOrUpdate(second);

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);
        await Assert.That(_results.Messages.Count).IsEqualTo(2).Because("Should be 1 updates");
        await Assert.That(_results.Data.Count).IsEqualTo(1).Because("Should be 1 item in the cache");

        var secondResult = _results.Messages[1].First();
        await Assert.That(secondResult.Previous.Value.UpdateCount).IsEqualTo(1).Because("Second Person should have received 1 update");
        await Assert.That(secondResult.Previous.Value.Completed).IsTrue().Because("Second person  should have received 1 update");
    }

    [Test]
    public async Task Watch()
    {
        var person = new Person("Adult1", 50);
        _source.AddOrUpdate(person);

        var result = new List<Change<Person, string>>(3);
        var watch = _watcher.Watch("Adult1").Subscribe(result.Add);

        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(50).Ticks);
        await Assert.That(result.Count).IsEqualTo(1).Because("Should be 1 updates");
        await Assert.That(result[0].Current).IsEqualTo(person).Because("Should be 1 item in the cache");

        _source.Edit(updater => updater.Remove(("Adult1")));
        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(500).Ticks);

        watch.Dispose();
    }

    [Test]
    public async Task WatchCompletesAfterPendingChangesWhenTheSourceCompletes()
    {
        using var source = new TestSourceCache<Person, string>(p => p.Key);
        using var watcher = source.Connect().AsWatcher(_scheduler);

        var received = new List<Change<Person, string>>();
        var receivedAtCompletion = 0;
        var isCompleted = false;
        using var subscription = watcher.Watch("Adult1").Subscribe(received.Add, () => { isCompleted = true; receivedAtCompletion = received.Count; });

        source.AddOrUpdate(new Person("Adult1", 50));
        source.Complete();
        _scheduler.Start();

        await Assert.That(isCompleted).IsTrue().Because("the source completed");
        await Assert.That(receivedAtCompletion).IsEqualTo(1).Because("the change was scheduled ahead of the completion, so it should arrive first");
    }

    [Test]
    public async Task WatchFailsWhenTheSourceFails()
    {
        using var source = new TestSourceCache<Person, string>(p => p.Key);
        using var watcher = source.Connect().AsWatcher(_scheduler);

        var actualError = default(Exception);
        var isCompleted = false;
        using var subscription = watcher.Watch("Adult1").Subscribe(static _ => { }, error => actualError = error, () => isCompleted = true);

        var expectedError = new Exception("Test Exception");
        source.SetError(expectedError);
        _scheduler.Start();

        await Assert.That(actualError).IsSameReferenceAs(expectedError).Because("a watch should be told when the source fails");
        await Assert.That(isCompleted).IsFalse().Because("the source failed, it did not complete");
    }

    [Test]
    public async Task WatchMany()
    {
        _source.AddOrUpdate(new Person("Adult1", 50));

        var result = new List<Change<Person, string>>(3);
        var watch1 = _watcher.Watch("Adult1").Subscribe(result.Add);
        var watch2 = _watcher.Watch("Adult1").Subscribe(result.Add);
        var watch3 = _watcher.Watch("Adult1").Subscribe(result.Add);
        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(100).Ticks);

        await Assert.That(result.Count).IsEqualTo(3).Because("Should be 3 updates");
        foreach (var update in result)
        {
            await Assert.That(update.Reason).IsEqualTo(ChangeReason.Add).Because("Change reason should be add");
        }

        result.Clear();

        _source.AddOrUpdate(new Person("Adult1", 51));
        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(500).Ticks);
        await Assert.That(result.Count).IsEqualTo(3).Because("Should be 3 updates");
        foreach (var update in result)
        {
            await Assert.That(update.Reason).IsEqualTo(ChangeReason.Update).Because("Change reason should be add");
        }

        result.Clear();

        _source.Remove("Adult1");
        _scheduler.AdvanceBy(TimeSpan.FromMilliseconds(500).Ticks);
        await Assert.That(result.Count).IsEqualTo(3).Because("Should be 3 updates");
        foreach (var update in result)
        {
            await Assert.That(update.Reason).IsEqualTo(ChangeReason.Remove).Because("Change reason should be add");
        }

        result.Clear();

        watch1.Dispose();
        watch2.Dispose();
        watch3.Dispose();
    }

    [Test]
    public async Task WatchStartedAfterTheSourceCompletedReceivesTheCurrentValueThenCompletes()
    {
        using var source = new TestSourceCache<Person, string>(p => p.Key);
        using var watcher = source.Connect().AsWatcher(_scheduler);

        var person = new Person("Adult1", 50);
        source.AddOrUpdate(person);
        source.Complete();
        _scheduler.Start();

        var received = new List<Change<Person, string>>();
        var isCompleted = false;
        using var subscription = watcher.Watch(person.Key).Subscribe(received.Add, () => isCompleted = true);

        await Assert.That(received).HasSingleItem().Because("the watch starts with the current value");
        await Assert.That(received[0].Current).IsEqualTo(person);
        await Assert.That(isCompleted).IsTrue().Because("a watch on a finished source can never receive anything more");
    }
}
