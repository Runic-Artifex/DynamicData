namespace DynamicData.Tests;

public class BufferInitialLifecycleFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WindowStartsAtFirstActualDataAndPreservesOccurrences(bool keyed)
    {
        var scheduler = new TestScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var messages = new List<IChangeSet>();
        using var subscription = source.Output.Subscribe(messages.Add);
        source.Send(null);
        scheduler.AdvanceBy(TimeSpan.FromMinutes(1).Ticks);
        source.Send(1);
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(500).Ticks);
        source.Send(1);
        source.Send(2);
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(499).Ticks);
        await Assert.That(messages).IsEmpty();
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(1).Ticks);
        await Assert.That(messages.Count).IsEqualTo(1);
        await Assert.That(Values(messages[0])).IsEquivalentTo(new[] { 1, 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.Send(3);
        source.Send(null);
        await Assert.That(messages.Count).IsEqualTo(2);
        await Assert.That(Values(messages[1])).IsEquivalentTo(new[] { 3 });
        await Assert.That(source.Subscriptions).IsEqualTo(1);
        scheduler.AdvanceBy(TimeSpan.FromDays(1).Ticks);
        await Assert.That(messages.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CompletionFlushesExactlyOnceAndCancelsTimer(bool keyed)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var events = new List<string>();
        using var subscription = source.Output.Subscribe(changes => events.Add(string.Join(",", Values(changes))), () => events.Add("completed"));
        source.Send(1);
        source.Send(2);
        source.Complete();
        await Assert.That(events).IsEquivalentTo(new[] { "1,2", "completed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(scheduler.ScheduledActions.Count).IsEqualTo(1);
        await Assert.That(scheduler.ScheduledActions[0].HasBeenCancelled).IsTrue();
        scheduler.ScheduledActions[0].Invoke(); // A callback that already escaped cancellation.
        await Assert.That(events.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyCompletionNeedsNoTimer(bool keyed)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromTicks(1), scheduler);
        var completed = 0;
        var messages = new List<IChangeSet>();
        using var subscription = source.Output.Subscribe(messages.Add, () => completed++);
        source.Send(null);
        source.Complete();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(messages).IsEmpty();
        await Assert.That(scheduler.ScheduledActions).IsEmpty();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ErrorDiscardsPendingBufferAndCancelsTimer(bool keyed, bool hasData)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var expected = new InvalidOperationException("source failed");
        Exception? error = null;
        var completed = false;
        var messages = new List<IChangeSet>();
        using var subscription = source.Output.Subscribe(messages.Add, ex => error = ex, () => completed = true);
        if (hasData)
        {
            source.Send(1);
        }

        source.Error(expected);
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
        foreach (var action in scheduler.ScheduledActions)
        {
            await Assert.That(action.HasBeenCancelled).IsTrue();
            action.Invoke();
        }

        await Assert.That(messages).IsEmpty();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisposalDropsPendingBufferAndIgnoresLateCallback(bool keyed)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var messages = new List<IChangeSet>();
        var completed = false;
        var subscription = source.Output.Subscribe(messages.Add, () => completed = true);
        source.Send(1);
        subscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(scheduler.ScheduledActions[0].HasBeenCancelled).IsTrue();
        scheduler.ScheduledActions[0].Invoke();
        source.Send(2);
        source.Complete();
        await Assert.That(messages).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ZeroForwardsImmediatelyWithoutScheduling(bool keyed)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.Zero, scheduler);
        var messages = new List<IChangeSet>();
        using var subscription = source.Output.Subscribe(messages.Add);
        source.Send(null);
        source.Send(1);
        source.Send(2);
        await Assert.That(messages.Count).IsEqualTo(2);
        await Assert.That(scheduler.ScheduledActions).IsEmpty();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task NegativeDurationIsRejectedBeforeSubscription(bool keyed)
    {
        await Assert.That(() => new Source(keyed, TimeSpan.FromTicks(-1), new TestScheduler())).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReentrantUpdatesAndCompletionAreSerializedAfterInitialFlush(bool keyed)
    {
        var scheduler = new TestScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var events = new List<string>();
        var depth = 0;
        var maxDepth = 0;
        using var subscription = source.Output.Subscribe(changes =>
        {
            maxDepth = Math.Max(maxDepth, ++depth);
            events.Add(string.Join(",", Values(changes)));
            if (events.Count == 1)
            {
                source.Send(2);
                source.Complete();
            }

            depth--;
        }, () => events.Add("completed"));
        source.Send(1);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(events).IsEquivalentTo(new[] { "1", "2", "completed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(maxDepth).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SynchronousTimerAndReentrantDisposalDoNotLeakOwnership(bool keyed)
    {
        var scheduler = new InitialCountingScheduler(Scheduler.Immediate);
        using var source = new Source(keyed, TimeSpan.FromTicks(1), scheduler);
        var messages = new List<IChangeSet>();
        IDisposable? subscription = null;
        subscription = source.Output.Subscribe(changes =>
        {
            messages.Add(changes);
            subscription!.Dispose();
        });
        source.Send(1);
        source.Send(2);
        await Assert.That(messages.Count).IsEqualTo(1);
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(scheduler.ScheduleCount).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TinyRealTimerSchedulesOnceWithBoundedAllocation(bool keyed)
    {
        var scheduler = new InitialCountingScheduler(Scheduler.Default);
        using var source = new Source(keyed, TimeSpan.FromTicks(1), scheduler);
        var flushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = source.Output.Subscribe(_ => flushed.TrySetResult());
        // An idle/empty source must not start a recurring sub-millisecond timer.
        source.Send(null);
        await Task.Delay(30);
        await Assert.That(scheduler.ScheduleCount).IsEqualTo(0);
        source.Send(1);
        await flushed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(30);
        source.Send(2);
        await Assert.That(scheduler.ScheduleCount).IsEqualTo(1);
        await Assert.That(scheduler.SchedulingAllocation).IsLessThan(256_000L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ObserverFailureDuringTimerFlushReleasesSourceAndTimer(bool keyed)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        using var subscription = source.Output.Subscribe(_ => throw new InvalidOperationException("observer failed"));
        source.Send(1);
        await Assert.That(() => scheduler.ScheduledActions[0].Invoke()).Throws<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(scheduler.ScheduledActions[0].HasBeenCancelled).IsTrue();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ObserverFailureDuringCompletionOrPassthroughReleasesOwnership(bool keyed, bool completion)
    {
        var scheduler = new InitialManualScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        using var subscription = source.Output.Subscribe(changes =>
        {
            if (completion || Values(changes).Contains(2))
            {
                throw new InvalidOperationException("observer failed");
            }
        });
        source.Send(1);
        if (completion)
        {
            await Assert.That(source.Complete).Throws<InvalidOperationException>();
        }
        else
        {
            scheduler.ScheduledActions[0].Invoke();
            await Assert.That(() => source.Send(2)).Throws<InvalidOperationException>();
        }

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(scheduler.ScheduledActions[0].HasBeenCancelled).IsTrue();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task TerminalAfterWindowPreservesAlreadyDeliveredChanges(bool keyed, bool fail)
    {
        var scheduler = new TestScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var events = new List<string>();
        using var subscription = source.Output.Subscribe(changes => events.Add(string.Join(",", Values(changes))), _ => events.Add("error"), () => events.Add("completed"));
        source.Send(1);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        source.Send(2);
        if (fail)
        {
            source.Error(new InvalidOperationException());
        }
        else
        {
            source.Complete();
        }

        await Assert.That(events).IsEquivalentTo(new[] { "1", "2", fail ? "error" : "completed" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EachSubscriptionOwnsItsWindowIncludingResubscription(bool keyed)
    {
        var scheduler = new TestScheduler();
        using var source = new Source(keyed, TimeSpan.FromSeconds(1), scheduler);
        var first = new List<IChangeSet>();
        var second = new List<IChangeSet>();
        var fresh = new List<IChangeSet>();
        using var subscription = source.Output.Subscribe(first.Add);
        using var other = source.Output.Subscribe(second.Add);
        source.Send(1);
        subscription.Dispose();
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        using var resubscription = source.Output.Subscribe(fresh.Add);
        source.Send(2);
        await Assert.That(first).IsEmpty();
        await Assert.That(second.Count).IsEqualTo(2);
        await Assert.That(fresh).IsEmpty();
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(Values(fresh.Single())).IsEquivalentTo(new[] { 2 });
        await Assert.That(source.Subscriptions).IsEqualTo(3);
    }

    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task SynchronousColdSourceReleasesDisposableReturnedAfterTermination(bool keyed, int terminal)
    {
        var acquired = 0;
        var released = 0;
        IScheduler scheduler = terminal == 0 ? Scheduler.Immediate : new InitialManualScheduler();
        var output = keyed
            ? Cold<IChangeSet<int, int>>(new ChangeSet<int, int> { new(ChangeReason.Add, 1, 1) })
                .BufferInitial(TimeSpan.FromTicks(1), scheduler).Select(changes => (IChangeSet)changes)
            : Cold<IChangeSet<int>>(new ChangeSet<int> { new(ListChangeReason.Add, 1) })
                .BufferInitial(TimeSpan.FromTicks(1), scheduler).Select(changes => (IChangeSet)changes);
        if (terminal == 0)
        {
            output = output.Take(1);
        }

        var messages = new List<IChangeSet>();
        var completed = false;
        Exception? error = null;
        using var subscription = output.Subscribe(messages.Add, ex => error = ex, () => completed = true);
        await Assert.That(acquired).IsEqualTo(1);
        await Assert.That(released).IsEqualTo(1);
        await Assert.That(messages.Count).IsEqualTo(terminal == 2 ? 0 : 1);
        await Assert.That(completed).IsEqualTo(terminal != 2);
        if (terminal == 2)
        {
            await Assert.That(error).IsNotNull();
        }
        else
        {
            await Assert.That(error).IsNull();
            await Assert.That(Values(messages[0])).IsEquivalentTo(new[] { 1 });
        }

        IObservable<TChangeSet> Cold<TChangeSet>(TChangeSet first) => Observable.Create<TChangeSet>(observer =>
        {
            acquired++;
            observer.OnNext(first);
            if (terminal == 1)
            {
                observer.OnCompleted();
            }
            else if (terminal == 2)
            {
                observer.OnError(new InvalidOperationException("source failed"));
            }

            return Disposable.Create(() => released++);
        });
    }

    private static IEnumerable<int> Values(IChangeSet changes) => changes switch
    {
        IChangeSet<int, int> cache => cache.Select(change => change.Current),
        IChangeSet<int> list => list.Select(change => change.Item.Current),
        _ => throw new InvalidOperationException()
    };

    private sealed class Source : IDisposable
    {
        private readonly ReactiveUI.Primitives.Signals.Signal<int?> _source = new();

        public Source(bool keyed, TimeSpan duration, IScheduler scheduler)
        {
            var input = Observable.Create<int?>(observer =>
            {
                Subscriptions++;
                return _source.Subscribe(observer);
            });
            Output = keyed
                ? input.Select(value => (IChangeSet<int, int>)(value.HasValue ? new ChangeSet<int, int> { new(ChangeReason.Add, value.Value, value.Value) } : new ChangeSet<int, int>()))
                    .BufferInitial(duration, scheduler).Select(changes => (IChangeSet)changes)
                : input.Select(value => (IChangeSet<int>)(value.HasValue ? new ChangeSet<int> { new(ListChangeReason.Add, value.Value) } : new ChangeSet<int>()))
                    .BufferInitial(duration, scheduler).Select(changes => (IChangeSet)changes);
        }

        public IObservable<IChangeSet> Output { get; }

        public int Subscriptions { get; private set; }

        public bool HasObservers => _source.HasObservers;

        public void Send(int? value) => _source.OnNext(value);

        public void Complete() => _source.OnCompleted();

        public void Error(Exception error) => _source.OnError(error);

        public void Dispose() => _source.Dispose();
    }
}

file sealed class InitialCountingScheduler(IScheduler inner) : IScheduler
{
    private int _scheduleCount;
    private long _schedulingAllocation;

    public int ScheduleCount => Volatile.Read(ref _scheduleCount);

    public long SchedulingAllocation => Interlocked.Read(ref _schedulingAllocation);

    public DateTimeOffset Now => inner.Now;

#if REACTIVE_TESTS
    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        Count(() => inner.Schedule(state, (_, value) => action(this, value)));

    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        Count(() => inner.Schedule(state, dueTime, (_, value) => action(this, value)));

    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action) =>
        Count(() => inner.Schedule(state, dueTime, (_, value) => action(this, value)));

    private IDisposable Count(Func<IDisposable> schedule)
    {
        Interlocked.Increment(ref _scheduleCount);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var result = schedule();
        Interlocked.Add(ref _schedulingAllocation, GC.GetAllocatedBytesForCurrentThread() - before);
        return result;
    }
#else
    public long Timestamp => inner.Timestamp;

    public void Schedule(IWorkItem item) => Count(() => inner.Schedule(item));

    public void Schedule(IWorkItem item, long dueTimestamp) => Count(() => inner.Schedule(item, dueTimestamp));

    private void Count(Action schedule)
    {
        Interlocked.Increment(ref _scheduleCount);
        var before = GC.GetAllocatedBytesForCurrentThread();
        schedule();
        Interlocked.Add(ref _schedulingAllocation, GC.GetAllocatedBytesForCurrentThread() - before);
    }
#endif
}

file sealed class InitialManualScheduler : IScheduler
{
    public List<ScheduledTimer> ScheduledActions { get; } = [];

    public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

#if REACTIVE_TESTS
    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        Schedule(state, TimeSpan.Zero, action);

    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action)
    {
        var cancelled = false;
        ScheduledActions.Add(new ScheduledTimer(() => action(this, state), () => cancelled));
        return Disposable.Create(() => cancelled = true);
    }

    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action) =>
        Schedule(state, dueTime - Now, action);
#else
    public long Timestamp => 0;

    public void Schedule(IWorkItem item) => Schedule(item, 0);

    public void Schedule(IWorkItem item, long dueTimestamp) =>
        ScheduledActions.Add(new ScheduledTimer(item.Execute, () => item is ReactiveUI.Primitives.Disposables.IsDisposed disposed && disposed.IsDisposed));
#endif

    public sealed class ScheduledTimer(Action invoke, Func<bool> cancelled)
    {
        public bool HasBeenCancelled => cancelled();

        public void Invoke() => invoke();
    }
}
