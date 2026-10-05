using System.Collections.Concurrent;

namespace DynamicData.Tests.List;

public class TransformManyQueueFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentCrossWritesFinishWhileBothObserversAreActive(bool childWrites)
    {
        var first = new DirectSource<IChangeSet<DirectList<int>>>();
        var second = new DirectSource<IChangeSet<DirectList<int>>>();
        using var firstChild = new DirectList<int>(1, 1);
        using var secondChild = new DirectList<int>(2, 2);
        using var rendezvous = new Barrier(2);
        var crossWrites = new ConcurrentBag<Task>();
        var firstEntered = 0;
        var secondEntered = 0;
        var blockedWrites = 0;
        var armed = !childWrites;
        var firstItems = new List<int>();
        var secondItems = new List<int>();
        Exception? firstError = null;
        Exception? secondError = null;
        using var firstSubscription = first.TransformMany(child => (IObservableList<int>)child).Subscribe(changes =>
        {
            firstItems.Clone(changes);
            if (armed && Interlocked.Exchange(ref firstEntered, 1) == 0)
            {
                CrossWrite(() =>
                {
                    if (childWrites) secondChild.Add(3);
                    else second.Push(Add(new DirectList<int>(3)));
                });
            }
        }, error => firstError = error);
        using var secondSubscription = second.TransformMany(child => (IObservableList<int>)child).Subscribe(changes =>
        {
            secondItems.Clone(changes);
            if (armed && Interlocked.Exchange(ref secondEntered, 1) == 0)
            {
                CrossWrite(() =>
                {
                    if (childWrites) firstChild.Add(4);
                    else first.Push(Add(new DirectList<int>(4)));
                });
            }
        }, error => secondError = error);
        if (childWrites)
        {
            first.Push(Add(firstChild));
            second.Push(Add(secondChild));
            armed = true;
        }

        var firstWriter = Start(() => { if (childWrites) firstChild.Add(5); else first.Push(Add(firstChild)); });
        var secondWriter = Start(() => { if (childWrites) secondChild.Add(6); else second.Push(Add(secondChild)); });
        // A bounded wait inside each callback releases the old monitor gates on failure.
        // Join every producer before disposal, so the pre-fix proof never leaves deadlocked threads.
        await Task.WhenAll(firstWriter, secondWriter).WaitAsync(TimeSpan.FromSeconds(20));
        await Task.WhenAll(crossWrites).WaitAsync(TimeSpan.FromSeconds(20));
        await Assert.That(firstError).IsNull();
        await Assert.That(secondError).IsNull();
        await Assert.That(firstEntered).IsEqualTo(1);
        await Assert.That(secondEntered).IsEqualTo(1);
        await Assert.That(blockedWrites).IsEqualTo(0).Because("opposing writes must return while both delivery callbacks still own their frames");
        await Assert.That(firstItems).IsEquivalentTo(childWrites ? new[] { 1, 1, 5, 4 } : new[] { 1, 1, 4 });
        await Assert.That(secondItems).IsEquivalentTo(childWrites ? new[] { 2, 2, 6, 3 } : new[] { 2, 2, 3 });

        void CrossWrite(Action write)
        {
            if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10)))
            {
                throw new TimeoutException("Both observers must be active before crossing the streams.");
            }

            var task = Start(write);
            crossWrites.Add(task);
            if (!task.Wait(TimeSpan.FromSeconds(3)))
            {
                Interlocked.Increment(ref blockedWrites);
            }
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SynchronousSnapshotsRetainDuplicateOccurrencesAndWaitForLiveChild(bool synchronousChildCompletion)
    {
        using var child = new DirectList<int>(1, 1) { CompleteOnConnect = synchronousChildCompletion };
        var values = new List<int>();
        var completions = 0;
        Exception? error = null;
        using var subscription = Observable.Return<IChangeSet<DirectList<int>>>(Add(child))
            .TransformMany(item => (IObservableList<int>)item)
            .Subscribe(changes => values.Clone(changes), ex => error = ex, () => completions++);
        await Assert.That(values).IsEquivalentTo(new[] { 1, 1 });
        await Assert.That(completions).IsEqualTo(synchronousChildCompletion ? 1 : 0);
        if (!synchronousChildCompletion)
        {
            child.Add(1);
            await Assert.That(values).IsEquivalentTo(new[] { 1, 1, 1 });
            child.Complete();
        }

        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(error).IsNull();
        await Assert.That(child.HasObserver).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ChildFailureTerminatesOnceAndReleasesSubscription(bool synchronous)
    {
        var expected = new InvalidOperationException("child");
        using var child = new DirectList<int>(1, 1) { ErrorOnConnect = synchronous ? expected : null };
        var errors = new List<Exception>();
        var completions = 0;
        var messages = 0;
        using var subscription = Observable.Return<IChangeSet<DirectList<int>>>(Add(child))
            .TransformMany(item => (IObservableList<int>)item)
            .Subscribe(_ => messages++, errors.Add, () => completions++);
        if (!synchronous)
        {
            child.Fail(expected);
        }

        var countAtError = messages;
        child.Add(2);
        child.Complete();
        await Assert.That(errors).IsEquivalentTo(new[] { expected });
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(messages).IsEqualTo(countAtError);
        await Assert.That(child.HasObserver).IsFalse();
    }

    [Test]
    public async Task ParentFailureReleasesLiveChildrenAndRejectsLaterNotifications()
    {
        var parent = new DirectSource<IChangeSet<DirectList<int>>>();
        using var child = new DirectList<int>(1, 1);
        var errors = new List<Exception>();
        var messages = 0;
        using var subscription = parent.TransformMany(item => (IObservableList<int>)item)
            .Subscribe(_ => messages++, errors.Add);
        parent.Push(Add(child));
        var expected = new InvalidOperationException("parent");
        parent.Fail(expected);
        child.Add(3);
        await Assert.That(errors).IsEquivalentTo(new[] { expected });
        await Assert.That(messages).IsEqualTo(1);
        await Assert.That(child.HasObserver).IsFalse();
        await Assert.That(parent.HasObserver).IsFalse();
    }

    [Test]
    public async Task DisposalDuringSnapshotDeliveryRejectsEscapedChildDelivery()
    {
        var parent = new DirectSource<IChangeSet<DirectList<int>>>();
        using var child = new DirectList<int>(1, 1);
        var messages = 0;
        IDisposable? subscription = null;
        subscription = parent.TransformMany(item => (IObservableList<int>)item).Subscribe(_ =>
        {
            messages++;
            subscription!.Dispose();
        });
        parent.Push(Add(child));
        child.Add(3);
        await Assert.That(messages).IsEqualTo(1);
        await Assert.That(child.HasObserver).IsFalse();
        await Assert.That(parent.HasObserver).IsFalse();
        subscription.Dispose();
    }

    [Test]
    public async Task LiveReplacementAndRemovalReplayExactDuplicateMultiplicity()
    {
        using var parent = new SourceList<IObservableList<int>>();
        using var first = new DirectList<int>(1);
        using var replacement = new DirectList<int>(1);
        using var results = parent.Connect().TransformMany(item => item).AsAggregator();
        parent.Add(first);
        first.Add(1);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1 });
        parent.ReplaceAt(0, replacement);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1 });
        await Assert.That(first.HasObserver).IsFalse();
        first.Add(1);
        replacement.Add(1);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1 });
        parent.RemoveAt(0);
        await Assert.That(results.Data.Items).IsEmpty();
        await Assert.That(results.Messages.Select(changes => changes.TotalChanges)).IsEquivalentTo(new[] { 1, 1, 1, 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(replacement.HasObserver).IsFalse();
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task LiveChildRetainsEachRepeatedReferenceOccurrence()
    {
        var item = new object();
        using var parent = new SourceList<IObservableList<object>>();
        using var child = new DirectList<object>(item);
        using var replacement = new DirectList<object>(item, item, item);
        using var results = parent.Connect().TransformMany(list => list).AsAggregator();
        parent.Add(child);
        child.Add(item);
        parent.ReplaceAt(0, replacement);
        await Assert.That(results.Data.Count).IsEqualTo(3);
        await Assert.That(results.Data.Items.All(value => ReferenceEquals(value, item))).IsTrue();
        parent.Clear();
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(3);
        await Assert.That(results.Data.Items).IsEmpty();
        await Assert.That(results.Exception).IsNull();
    }

    private static Task Start(Action action) => Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private static ChangeSet<T> Add<T>(T value) where T : notnull => new() { new(ListChangeReason.Add, value) };

    private sealed class DirectSource<T> : IObservable<T>
    {
        private IObserver<T>? _observer;

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return Disposable.Create(() => _observer = null);
        }

        public bool HasObserver => _observer is not null;

        public void Push(T value) => _observer?.OnNext(value);

        public void Fail(Exception error) => _observer?.OnError(error);
    }

    private sealed class DirectList<T>(params T[] initial) : IObservableList<T> where T : notnull
    {
        private T[] _items = initial;
        private IObserver<IChangeSet<T>>? _observer;

        public bool CompleteOnConnect { get; init; }
        public Exception? ErrorOnConnect { get; init; }
        public bool HasObserver => _observer is not null;
        public int Count => _items.Length;
        public IObservable<int> CountChanged => Observable.Return(Count);
        public IReadOnlyList<T> Items => _items;

        public IObservable<IChangeSet<T>> Connect(Func<T, bool>? predicate = null) => Observable.Create<IChangeSet<T>>(observer =>
        {
            _observer = observer;
            if (_items.Length > 0)
            {
                observer.OnNext(new ChangeSet<T> { new(ListChangeReason.AddRange, _items, 0) });
            }

            if (ErrorOnConnect is not null)
            {
                observer.OnError(ErrorOnConnect);
            }
            else if (CompleteOnConnect)
            {
                observer.OnCompleted();
            }

            return Disposable.Create(() => _observer = null);
        });

        public IObservable<IChangeSet<T>> Preview(Func<T, bool>? predicate = null) => Observable.Never<IChangeSet<T>>();

        public void Add(T value)
        {
            var index = _items.Length;
            _items = [.. _items, value];
            _observer?.OnNext(new ChangeSet<T> { new(ListChangeReason.Add, value, index) });
        }

        public void Complete() => _observer?.OnCompleted();

        public void Fail(Exception error) => _observer?.OnError(error);

        public void Dispose() => _observer = null;
    }
}
