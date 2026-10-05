using System.Collections.Concurrent;
using Randomizer = Bogus.Randomizer;
#if REACTIVE_TESTS
using MaterializedNotificationKind = System.Reactive.NotificationKind;
#else
using MaterializedNotificationKind = ReactiveUI.Primitives.Core.SparkKind;
#endif
#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public class TransformAsyncFixture
{
    [Test]
    public async Task Add()
    {
        using var stub = new TransformStub();
        var person = new Person("Adult1", 50);
        stub.Source.AddOrUpdate(person);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(1).Because("Should be 1 updates");
        await Assert.That(stub.Results.Data.Count).IsEqualTo(1).Because("Should be 1 item in the cache");

        var firstPerson = await stub.TransformFactory(person);

        await Assert.That(stub.Results.Data.Items[0]).IsEqualTo(firstPerson).Because("Should be same person");
    }

    [Test]
    public async Task BatchOfUniqueUpdates()
    {
        var people = Enumerable.Range(1, 100).Select(i => new Person("Name" + i, i)).ToArray();
        using var stub = new TransformStub();
        stub.Source.AddOrUpdate(people);

        //     Thread.Sleep(10000);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(1).Because("Should be 1 updates");
        await Assert.That(stub.Results.Messages[0].Adds).IsEqualTo(100).Because("Should return 100 adds");

        var result = await Task.WhenAll(people.Select(stub.TransformFactory));
        var transformed = result.OrderBy(p => p.Age).ToArray();
        await Assert.That(stub.Results.Data.Items.OrderBy(p => p.Age)).IsEquivalentTo(transformed, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("each input should be transformed exactly once");
    }

    [Test]
    public async Task Clear()
    {
        using var stub = new TransformStub();
        var people = Enumerable.Range(1, 100).Select(l => new Person("Name" + l, l)).ToArray();

        stub.Source.AddOrUpdate(people);
        stub.Source.Clear();

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(2).Because("Should be 2 updates");
        await Assert.That(stub.Results.Messages[0].Adds).IsEqualTo(100).Because("Should be 80 adds");
        await Assert.That(stub.Results.Messages[1].Removes).IsEqualTo(100).Because("Should be 80 removes");
        await Assert.That(stub.Results.Data.Count).IsEqualTo(0).Because("Should be nothing cached");
    }

    [Test]
    public async Task HandleError()
    {
        using var stub = new TransformStub(p => throw new Exception("Broken"));
        stub.Source.AddOrUpdate(new Person("Name1", 1));

        await Assert.That(stub.Results.Error).IsNotNull();
    }

    [Test]
    public async Task Remove()
    {
        const string key = "Adult1";
        var person = new Person(key, 50);

        using var stub = new TransformStub();
        stub.Source.AddOrUpdate(person);
        stub.Source.Remove(key);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(2).Because("Should be 2 updates");
        await Assert.That(stub.Results.Messages[0].Adds).IsEqualTo(1);
        await Assert.That(stub.Results.Messages[1].Removes).IsEqualTo(1);
        await Assert.That(stub.Results.Data.Count).IsEqualTo(0).Because("Should be nothing cached");
    }

    [Test]
    public async Task RemoveFlowsToTheEnd()
    {
        const int count = 100;
        using var cache = new SourceCache<Person, string>(person => person.Name);
        var allowTransforms = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var adds = 0;
        var removes = 0;
        using var subscription = cache.Connect()
            .TransformAsync(async (Person person, ReactiveUI.Primitives.Optional<Person> previous, string key, CancellationToken token) =>
            {
                await allowTransforms.Task.WaitAsync(token);
                return person;
            })
            .Bind(out var collection)
            .Subscribe(changes =>
            {
                adds += changes.Adds;
                removes += changes.Removes;
                if (removes == count)
                    finished.TrySetResult();
            }, error => finished.TrySetException(error));

        for (var index = 0; index < count; index++)
        {
            var person = new Person("Name" + index, index);
            cache.AddOrUpdate(person);
            cache.RemoveKey(person.Name);
        }

        allowTransforms.SetResult();
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.That(adds).IsEqualTo(count);
        await Assert.That(removes).IsEqualTo(count);
        await Assert.That(collection).IsEmpty();
    }

    [Test]
    public async Task ReTransformAll()
    {
        var people = Enumerable.Range(1, 10).Select(i => new Person("Name" + i, i)).ToArray();
        var forceTransform = new ReactiveUI.Primitives.Signals.Signal<Unit>();

        using var stub = new TransformStub(forceTransform);
        stub.Source.AddOrUpdate(people);
        forceTransform.OnNext(Unit.Default);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(2);
        await Assert.That(stub.Results.Messages[1].Updates).IsEqualTo(10);

        for (var i = 1; i <= 10; i++)
        {
            var original = stub.Results.Messages[0].ElementAt(i - 1).Current;
            var updated = stub.Results.Messages[1].ElementAt(i - 1).Current;

            await Assert.That(updated).IsEqualTo(original);
            await Assert.That(ReferenceEquals(original, updated)).IsFalse();
        }
    }

    [Test]
    public async Task ReTransformSelected()
    {
        var people = Enumerable.Range(1, 10).Select(i => new Person("Name" + i, i)).ToArray();
        var forceTransform = new ReactiveUI.Primitives.Signals.Signal<Func<Person, bool>>();

        using var stub = new TransformStub(forceTransform);
        stub.Source.AddOrUpdate(people);
        forceTransform.OnNext(person => person.Age <= 5);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(2);
        await Assert.That(stub.Results.Messages[1].Updates).IsEqualTo(5);

        for (var i = 1; i <= 5; i++)
        {
            var original = stub.Results.Messages[0].ElementAt(i - 1).Current;
            var updated = stub.Results.Messages[1].ElementAt(i - 1).Current;
            await Assert.That(updated).IsEqualTo(original);
            await Assert.That(ReferenceEquals(original, updated)).IsFalse();
        }
    }

    [Test]
    public async Task SameKeyChanges()
    {
        using var stub = new TransformStub();
        var people = Enumerable.Range(1, 10).Select(i => new Person("Name", i)).ToArray();

        stub.Source.AddOrUpdate(people);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(1).Because("Should be 1 updates");
        await Assert.That(stub.Results.Messages[0].Adds).IsEqualTo(1).Because("Should return 1 adds");
        await Assert.That(stub.Results.Messages[0].Updates).IsEqualTo(9).Because("Should return 9 adds");
        await Assert.That(stub.Results.Data.Count).IsEqualTo(1).Because("Should result in 1 record");

        var lastTransformed = await stub.TransformFactory(people.Last());
        var onlyItemInCache = stub.Results.Data.Items[0];

        await Assert.That(onlyItemInCache).IsEqualTo(lastTransformed).Because("Incorrect transform result");
    }

    [Test]
    public async Task Update()
    {
        const string key = "Adult1";
        var newperson = new Person(key, 50);
        var updated = new Person(key, 51);

        using var stub = new TransformStub();
        stub.Source.AddOrUpdate(newperson);
        stub.Source.AddOrUpdate(updated);

        await Assert.That(stub.Results.Messages.Count).IsEqualTo(2).Because("Should be 2 updates");
        await Assert.That(stub.Results.Messages[0].Adds).IsEqualTo(1).Because("Should be 1 adds");
        await Assert.That(stub.Results.Messages[1].Updates).IsEqualTo(1).Because("Should be 1 update");
    }

    [Test, Arguments(true), Arguments(false)]
    public async Task TransformOnRefresh(bool transformOnRefresh)
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        using var results = source.Connect()
            .AutoRefresh()
            .TransformAsync((p, key) => Task.FromResult(new PersonWithAgeGroup(p, p.Age < 18 ? "Child" : "Adult")), TransformAsyncOptions.Default with { TransformOnRefresh = transformOnRefresh }).AsAggregator();

        var person = new Person("SomeOne", 16);
        source.AddOrUpdate(person);

        await Assert.That(results.Data.Count).IsEqualTo(1);
        await Assert.That(results.Data.Lookup("SomeOne").Value.AgeGroup).IsEqualTo("Child");

        person.Age = 21;

        await Assert.That(results.Data.Count).IsEqualTo(1);
        await Assert.That(results.Data.Lookup("SomeOne").Value.AgeGroup).IsEqualTo(transformOnRefresh ? "Adult" : "Child");

    }

    [Test]
    public async Task TransformAsyncCancelsTokenOnUnSubscribe()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        var tcs = new TaskCompletionSource<Person>();
        using var sub = source.Connect()
            .TransformAsync(async (c, p, key, cancel) =>
            {
                using (cancel.Register(() => tcs.SetCanceled(), useSynchronizationContext: false))
                {
                    return await tcs.Task.ConfigureAwait(false);
                }
            })
            .Subscribe();

        source.AddOrUpdate(new Person());

        sub.Dispose();
        await Assert.That(tcs.Task.IsCanceled).IsTrue();
    }


    [Test, Arguments(10), Arguments(100)]

    public async Task WithMaxConcurrency(int maxConcurrency)
    {
        /* We need to test whether the max concurrency has any effect.

             If  maxConcurrency == 100, this test takes a little more than 100 ms
             If maxConcurrency = 10, this test takes a little more than 1s

            So it works, but how can it be tested in a scientific way ??
        */

        const int transformCount = 100;

        using var source = new SourceCache<Person, string>(p => p.Name);
        using var results = source.Connect()
            .TransformAsync(async (p, key) =>
            {
                await Task.Delay(100);

                return new PersonWithAgeGroup(p, p.Age < 18 ? "Child" : "Adult");
            }, TransformAsyncOptions.Default with { MaximumConcurrency = maxConcurrency }).AsAggregator();

        source.AddOrUpdate(Enumerable.Range(1, transformCount).Select(l => new Person("Person" + l, l)));

        await results.Data.CountChanged.Where(c => c == transformCount).Take(1);
    }

    [Test]
    public async Task ForcedTransformCompletingAlongsideSourceUpdate_AreDeliveredSerially()
    {
        var timeout = TimeSpan.FromSeconds(30);

        using var source = new SourceCache<Person, string>(p => p.Name);
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();
        using var registered = new SemaphoreSlim(0);

        // One release handle per transform invocation, so each can be completed on command.
        var outstanding = new ConcurrentDictionary<string, ConcurrentQueue<TaskCompletionSource>>();

        var published = source.Connect()
            .TransformAsync(
                async person =>
                {
                    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    outstanding.GetOrAdd(person.Name, static _ => new ConcurrentQueue<TaskCompletionSource>()).Enqueue(release);
                    registered.Release();

                    await release.Task;

                    return new PersonWithGender(person, person.Age % 2 == 0 ? "M" : "F");
                },
                force)
            .ValidateSynchronization()

            // ValidateSynchronization tracks the whole in-flight period of a notification,
            // including downstream work, so holding each one makes an overlapping delivery
            // observable instead of something that has to be caught in a sub-microsecond window.
            .Do(static _ => Thread.Sleep(100))
            .Publish();

        var terminal = published.Materialize().LastAsync().ToTask();
        using var results = published.AsAggregator();
        using var connection = published.Connect();

        // Seed a single item, so that the forced pass has something to re-transform.
        source.AddOrUpdate(new Person("Seed", 2));
        await Assert.That((await registered.WaitAsync(timeout))).IsTrue().Because("the seed transform should have started");
        await Release(outstanding, "Seed");
        await results.Data.CountChanged.Where(static count => count == 1).Take(1);

        // Leave one transform outstanding on each chain: the forced pass re-transforms the seed,
        // and the source update introduces a second item through the other chain.
        force.OnNext(static (_, _) => true);
        await Assert.That((await registered.WaitAsync(timeout))).IsTrue().Because("the forced transform should have started");

        source.AddOrUpdate(new Person("Added", 4));
        await Assert.That((await registered.WaitAsync(timeout))).IsTrue().Because("the source transform should have started");

        // Release both at once. Each chain applies its cache updates and emits on whichever thread
        // completed it, so this is the moment the two can collide.
        using (var barrier = new Barrier(3))
        {
            var forced = Task.Run(async () =>
            {
                barrier.SignalAndWait();
                await Release(outstanding, "Seed");
            });

            var added = Task.Run(async () =>
            {
                barrier.SignalAndWait();
                await Release(outstanding, "Added");
            });

            barrier.SignalAndWait();
            await Task.WhenAll(forced, added);
        }

        // Both merge inputs have to complete before the merged sequence does.
        force.OnCompleted();
        source.Dispose();

        var lastNotification = await terminal;

        await Assert.That(lastNotification.Exception).IsNull().Because("a forced transform and a source update must never be delivered concurrently");
        await Assert.That(lastNotification.Kind).IsEqualTo(MaterializedNotificationKind.OnCompleted).Because("the sequence should end by completing, not by faulting");

        static async Task Release(ConcurrentDictionary<string, ConcurrentQueue<TaskCompletionSource>> outstanding, string name)
        {
            await Assert.That(outstanding[name].TryDequeue(out var release)).IsTrue().Because($"a transform for {name} should be outstanding");
            release!.SetResult();
        }
    }

    // Serialization has to hold under arbitrary interleaving, not only the one scripted collision
    // above. Several writers drive the source while another drives forced passes, and every
    // notification is checked both for overlap and for structural integrity, since the two chains
    // also share the cache that produces those change sets.
    [Test]
    public async Task ForcedTransformsUnderConcurrentLoad_AreDeliveredSerially()
    {
        const int writerCount = 3;
        const int seedCount = 5;

        var randomizer = new Randomizer(0x1097);
        var iterations = randomizer.Int(150, 250);
        var timeout = TimeSpan.FromMinutes(2);

        using var source = new SourceCache<Person, string>(p => p.Name);
        using var force = new ReactiveUI.Primitives.Signals.Signal<Func<Person, string, bool>>();

        var published = source.Connect()
            .TransformAsync(
                async person =>
                {
                    await Task.Yield();
                    return new PersonWithGender(person, person.Age % 2 == 0 ? "M" : "F");
                },
                force)
            .ValidateSynchronization()
            .ValidateChangeSets(static personWithGender => personWithGender.Name)

            // Each notification is held for a moment so that an overlapping delivery is actually
            // observed, rather than passing through a window too narrow to catch.
            .Do(static _ => Thread.SpinWait(2_000))
            .Publish();

        var terminal = published.Materialize().LastAsync().ToTask();
        using var connection = published.Connect();

        var names = Enumerable.Range(1, seedCount).Select(i => "Name" + i).ToArray();
        source.AddOrUpdate(names.Select((name, i) => new Person(name, i + 1)));

        // The main thread joins the barrier so every writer starts at the same moment.
        using var barrier = new Barrier(writerCount + 2);

        var writers = Enumerable.Range(0, writerCount)
            .Select(writer => Task.Run(() =>
            {
                var writerRandomizer = new Randomizer(0x1097 + writer + 1);
                barrier.SignalAndWait();

                for (var i = 0; i < iterations; i++)
                {
                    source.AddOrUpdate(new Person(writerRandomizer.ArrayElement(names), writerRandomizer.Int(1, 80)));
                }
            }))
            .ToArray();

        var forcer = Task.Run(() =>
        {
            barrier.SignalAndWait();

            for (var i = 0; i < iterations; i++)
            {
                force.OnNext(static (_, _) => true);
            }
        });

        barrier.SignalAndWait();
        await Task.WhenAll(writers.Append(forcer));

        // Both merge inputs have to complete before the merged sequence does.
        force.OnCompleted();
        source.Dispose();

        await Assert.That((await Task.WhenAny(terminal, Task.Delay(timeout)))).IsSameReferenceAs(terminal).Because("the pipeline should drain rather than deadlock");

        var lastNotification = await terminal;

        await Assert.That(lastNotification.Exception).IsNull().Because("deliveries must neither overlap nor carry inconsistent change sets, however the writers interleave");
        await Assert.That(lastNotification.Kind).IsEqualTo(MaterializedNotificationKind.OnCompleted).Because("the sequence should end by completing, not by faulting");
    }

    private class TransformStub : IDisposable
    {
        public TransformStub()
        {
            TransformFactory = (p) =>
            {
                var result = new PersonWithGender(p, p.Age % 2 == 0 ? "M" : "F");
                return Task.FromResult(result);
            };

            Results = new ChangeSetAggregator<PersonWithGender, string>(Source.Connect().TransformAsync(TransformFactory));
        }

        public TransformStub(Func<Person, PersonWithGender> factory)
        {
            TransformFactory = (p) =>
            {
                var result = factory(p);
                return Task.FromResult(result);
            };

            Results = new ChangeSetAggregator<PersonWithGender, string>(Source.Connect().TransformAsync(TransformFactory));
        }

        public TransformStub(IObservable<Unit> retransformer)
        {
            TransformFactory = (p) =>
            {
                var result = new PersonWithGender(p, p.Age % 2 == 0 ? "M" : "F");
                return Task.FromResult(result);
            };

            Results = new ChangeSetAggregator<PersonWithGender, string>(
                Source.Connect().TransformAsync(
                    TransformFactory,
                    retransformer.Select(
                        x =>
                        {
                            Func<Person, string, bool> transformer = (p, key) => true;
                            return transformer;
                        })));
        }

        public TransformStub(IObservable<Func<Person, bool>> retransformer)
        {
            TransformFactory = (p) =>
            {
                var result = new PersonWithGender(p, p.Age % 2 == 0 ? "M" : "F");
                return Task.FromResult(result);
            };

            Results = new ChangeSetAggregator<PersonWithGender, string>(
                Source.Connect().TransformAsync(
                    TransformFactory,
                    retransformer.Select(
                        selector =>
                        {
                            Func<Person, string, bool> transformed = (p, key) => selector(p);
                            return transformed;
                        })));
        }

        public ChangeSetAggregator<PersonWithGender, string> Results { get; }

        public ISourceCache<Person, string> Source { get; } = new SourceCache<Person, string>(p => p.Name);

        public Func<Person, Task<PersonWithGender>> TransformFactory { get; }

        public void Dispose()
        {
            Source.Dispose();
            Results.Dispose();
        }
    }
}
