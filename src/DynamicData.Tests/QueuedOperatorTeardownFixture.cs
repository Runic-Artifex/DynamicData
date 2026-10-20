namespace DynamicData.Tests;

/// <summary>
/// Disposal of operators that tear down the items they track (list <c>DisposeMany</c>, cache <c>OnItemRemoved</c> and
/// <c>AsyncDisposeMany</c>). Disposing does not wait for a delivery in progress on another thread; that thread tears
/// the items down once its delivery returns, so no item is torn down while a delivery may use it, and every
/// remaining item is torn down exactly once.
/// </summary>
public class QueuedOperatorTeardownFixture
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    public static IEnumerable<string> Operations() => ["list-dispose-many", "cache-on-item-removed", "cache-async-dispose-many"];

    [Test]
    [MethodDataSource(nameof(Operations))]
    public async Task DisposeDuringDeliveryOnAnotherThreadDefersTeardownUntilDeliveryReturns(string operation)
    {
        using var teardownCase = TeardownCase.Create(operation);
        var first = new Owned();
        var second = new Owned();
        teardownCase.Add(first);

        var entered = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var updates = 0;
        var tornDownWhileDelivering = -1;
        var subscription = teardownCase.Emissions.Subscribe(_ =>
        {
            if (Interlocked.Increment(ref updates) == 2)
            {
                entered.Set();
                release.Wait(Timeout);
                tornDownWhileDelivering = first.TeardownCount + second.TeardownCount;
            }
        });

        var producer = StartDedicatedThread(() => teardownCase.Add(second));
        await RunOnDedicatedThread(() =>
        {
            if (!entered.Wait(Timeout))
            {
                throw new TimeoutException("The delivery did not start.");
            }
        });

        // Dispose returns while the delivery that holds the items is still running.
        await RunOnDedicatedThread(subscription.Dispose);
        var tornDownBeforeRelease = first.TeardownCount + second.TeardownCount;
        var completedBeforeRelease = teardownCase.DisposalsCompleted;
        release.Set();
        await producer.WaitAsync(Timeout);

        await Assert.That(tornDownBeforeRelease).IsEqualTo(0);
        await Assert.That(completedBeforeRelease).IsEqualTo(operation != "cache-async-dispose-many");
        await Assert.That(tornDownWhileDelivering).IsEqualTo(0);
        await Assert.That(first.TeardownCount).IsEqualTo(1);
        await Assert.That(second.TeardownCount).IsEqualTo(1);
        await Assert.That(teardownCase.DisposalsCompleted).IsTrue();
    }

    [Test]
    [MethodDataSource(nameof(Operations))]
    public async Task DisposeInsideDeliveryTearsDownEveryItemOnce(string operation)
    {
        using var teardownCase = TeardownCase.Create(operation);
        var first = new Owned();
        var second = new Owned();
        teardownCase.Add(first);

        var updates = 0;
        var tornDownWhenDisposeReturned = -1;
        IDisposable? subscription = null;
        subscription = teardownCase.Emissions.Subscribe(_ =>
        {
            if (Interlocked.Increment(ref updates) == 2)
            {
                subscription!.Dispose();
                tornDownWhenDisposeReturned = first.TeardownCount + second.TeardownCount;
            }
        });

        await RunOnDedicatedThread(() => teardownCase.Add(second));

        // Synchronous teardowns still run inside Dispose; AsyncDisposeMany finishes the delivery first, so the
        // items it replaces are disposed before its disposals complete.
        await Assert.That(tornDownWhenDisposeReturned).IsEqualTo(operation == "cache-async-dispose-many" ? 0 : 2);
        await Assert.That(updates).IsEqualTo(2);
        await Assert.That(first.TeardownCount).IsEqualTo(1);
        await Assert.That(second.TeardownCount).IsEqualTo(1);
        await Assert.That(teardownCase.DisposalsCompleted).IsTrue();
    }

    [Test]
    [MethodDataSource(nameof(RacingCases))]
    public async Task RacingDisposeCallsTearDownEveryItemOnce(string operation, bool duringDelivery)
    {
        const int Disposers = 8;
        using var teardownCase = TeardownCase.Create(operation);
        var items = Enumerable.Range(0, 4).Select(_ => new Owned()).ToArray();
        foreach (var item in items)
        {
            teardownCase.Add(item);
        }

        var entered = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var last = new Owned();
        var subscription = teardownCase.Emissions.Subscribe(count =>
        {
            if (duringDelivery && count == items.Length + 1)
            {
                entered.Set();
                release.Wait(Timeout);
            }
        });

        Task producer = Task.CompletedTask;
        if (duringDelivery)
        {
            producer = StartDedicatedThread(() => teardownCase.Add(last));
            await RunOnDedicatedThread(() =>
            {
                if (!entered.Wait(Timeout))
                {
                    throw new TimeoutException("The delivery did not start.");
                }
            });
        }

        using var barrier = new Barrier(Disposers);
        var disposers = Enumerable.Range(0, Disposers).Select(_ => StartDedicatedThread(() =>
        {
            barrier.SignalAndWait(Timeout);
            subscription.Dispose();
        })).ToArray();
        await Task.WhenAll(disposers).WaitAsync(Timeout);
        release.Set();
        await producer.WaitAsync(Timeout);

        foreach (var item in items)
        {
            await Assert.That(item.TeardownCount).IsEqualTo(1);
        }

        await Assert.That(last.TeardownCount).IsEqualTo(duringDelivery ? 1 : 0);
        await Assert.That(teardownCase.DisposalsCompleted).IsTrue();
    }

    public static IEnumerable<(string, bool)> RacingCases()
        => Operations().SelectMany(static operation => new[] { (operation, false), (operation, true) });

    private static Task StartDedicatedThread(Action action)
        => Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private static Task RunOnDedicatedThread(Action action)
        => StartDedicatedThread(action).WaitAsync(Timeout);

    /// <summary>An item that counts its teardowns: disposal, or the remove action that disposes it.</summary>
    private sealed class Owned : IDisposable
    {
        private static int _nextId;
        private int _teardownCount;

        public int Id { get; } = Interlocked.Increment(ref _nextId);

        public int TeardownCount => Volatile.Read(ref _teardownCount);

        public void Dispose() => Interlocked.Increment(ref _teardownCount);
    }

    /// <param name="Emissions">The operator's notifications, as the number of items it holds.</param>
    /// <param name="Add">Adds an item to the source.</param>
    /// <param name="Owner">Owns the source.</param>
    /// <param name="Completed">Whether the operator's disposals have completed; always true unless asynchronous.</param>
    private sealed record TeardownCase(IObservable<int> Emissions, Action<Owned> Add, IDisposable Owner, Func<bool> Completed) : IDisposable
    {
        public bool DisposalsCompleted => Completed();

        public static TeardownCase Create(string operation)
        {
            switch (operation)
            {
                case "list-dispose-many":
                    {
                        var source = new SourceList<Owned>();
                        var held = 0;
                        return new(
                            source.Connect().DisposeMany().Select(changes => held += changes.Adds - changes.Removes).Where(static count => count != 0),
                            source.Add,
                            source,
                            static () => true);
                    }

                case "cache-on-item-removed":
                    {
                        var source = new SourceCache<Owned, int>(static item => item.Id);
                        return new(
                            source.Connect().OnItemRemoved(static item => item.Dispose()).Select(_ => source.Count).Where(static count => count != 0),
                            source.AddOrUpdate,
                            source,
                            static () => true);
                    }

                case "cache-async-dispose-many":
                    {
                        var source = new SourceCache<Owned, int>(static item => item.Id);
                        var completed = 0;
                        return new(
                            source.Connect()
                                .AsyncDisposeMany(disposalsCompleted => disposalsCompleted.Subscribe(_ => { }, () => Volatile.Write(ref completed, 1)))
                                .Select(_ => source.Count)
                                .Where(static count => count != 0),
                            source.AddOrUpdate,
                            source,
                            () => Volatile.Read(ref completed) != 0);
                    }

                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }
        }

        public void Dispose() => Owner.Dispose();
    }
}
