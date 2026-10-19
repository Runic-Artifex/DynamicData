namespace DynamicData.Tests.List;

public class ListCreationFixtures
{
    [Test]
    public async Task Create()
    {
        static Task<T> CreateTask<T>(T value) => Task.FromResult(value);

        await SubscribeAndAssert(
            ObservableChangeSet.Create<int>(
                async list =>
                {
                    var value = await CreateTask(10);
                    list.Add(value);
                    return () => { };
                }));
    }

    [Test]
    public async Task TaskCompletesTheStreamAfterItsChanges()
    {
        var task = new TaskCompletionSource();
        ISourceList<int>? captured = null;

        using var subscription = ObservableChangeSet.Create<int>((list, _) => { captured = list; return task.Task; }).RecordListItems(out var results);

        captured!.Add(10);
        await Assert.That(results.HasCompleted).IsFalse().Because("the task has not completed yet");

        task.SetResult();

        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 10 }).Because("changes made by the task arrive ahead of completion");
        await Assert.That(results.HasCompleted).IsTrue().Because("nothing can follow the end of the task");
        await Assert.That(results.Error).IsNull();
    }

    [Test]
    public async Task TaskFailureIsDeliveredOnTheThreadThatFailsIt()
    {
        var task = new TaskCompletionSource();

        using var subscription = ObservableChangeSet.Create<int>((_, _) => task.Task).RecordListItems(out var results);

        var expectedError = new Exception("Test Exception");
        task.SetException(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the failure should arrive before SetException returns, not later on another thread");
    }

    [Test]
    public async Task TaskCompletingWhileAChangeIsBeingDeliveredCompletesAfterIt()
    {
        var task = new TaskCompletionSource();
        ISourceList<int>? captured = null;
        var completerReturned = false;
        var observer = new OverlapDetectingObserver<IChangeSet<int>>(_ =>
        {
            // Complete the task on another thread while this change is still being delivered.
            var completer = new Thread(() => task.SetResult());
            completer.Start();
            completerReturned = completer.Join(TimeSpan.FromSeconds(10));
        });

        using var subscription = ObservableChangeSet.Create<int>((list, _) => { captured = list; return task.Task; }).Subscribe(observer);
        captured!.Add(0);

        await observer.Terminated.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(completerReturned).IsTrue().Because("the completing thread hands the completion over instead of waiting for the delivery");
        await Assert.That(observer.MaxConcurrentCalls).IsEqualTo(1).Because("the completion must not overlap the change being delivered");
        await Assert.That(observer.HasCompleted).IsTrue();
        await Assert.That(observer.Values).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TaskOutcomeRacingConcurrentEditsIsSerializedAndLast(bool fail)
    {
        for (var iteration = 0; iteration < 10; iteration++)
        {
            var task = new TaskCompletionSource();
            var error = new InvalidOperationException("task failed");
            ISourceList<int>? captured = null;
            var observer = new OverlapDetectingObserver<IChangeSet<int>>(_ => Thread.SpinWait(200));
            using var subscription = ObservableChangeSet.Create<int>((list, _) => { captured = list; return task.Task; }).Subscribe(observer);

            var stop = 0;
            var editor = new Thread(() =>
            {
                try
                {
                    for (var i = 0; Volatile.Read(ref stop) == 0; i++)
                    {
                        captured!.Add(i);
                    }
                }
                catch (ObjectDisposedException)
                {
                    // The collection is disposed once the task has ended the stream.
                }
            });
            var completer = new Thread(() =>
            {
                SpinWait.SpinUntil(() => observer.Values >= 5, TimeSpan.FromSeconds(5));
                if (fail)
                {
                    task.SetException(error);
                }
                else
                {
                    task.SetResult();
                }
            });

            editor.Start();
            completer.Start();
            try
            {
                await observer.Terminated.WaitAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                Volatile.Write(ref stop, 1);
                editor.Join(TimeSpan.FromSeconds(10));
                completer.Join(TimeSpan.FromSeconds(10));
            }

            await Assert.That(observer.MaxConcurrentCalls).IsEqualTo(1).Because("notifications must never overlap");
            await Assert.That(observer.CallsAfterTerminal).IsEqualTo(0).Because("the terminal notification is the last one");
            await Assert.That(observer.TerminalCount).IsEqualTo(1);
            if (fail)
            {
                await Assert.That(observer.Error).IsSameReferenceAs(error);
            }
            else
            {
                await Assert.That(observer.HasCompleted).IsTrue();
            }
        }
    }

    [Test]
    public async Task TaskThatFinishesDuringSubscriptionStillDeliversItsChanges()
    {
        using var subscription = ObservableChangeSet.Create<int>((list, _) => { list.Add(10); return Task.CompletedTask; }).RecordListItems(out var results);

        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 10 }).Because("the change was made before the task finished");
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Error).IsNull();
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Accetable for test.")]
    private async Task SubscribeAndAssert<T>(IObservable<IChangeSet<T>> observableChangeset, bool expectsError = false)
        where T : notnull
    {
        Exception? error = null;
        var complete = false;
        IChangeSet<T>? changes = null;

        using (var myList = observableChangeset.Finally(() => complete = true).AsObservableList())
        using (myList.Connect().Subscribe(result => changes = result, ex => error = ex))
        {
            if (!expectsError)
            {
                await Assert.That(error).IsNull();
            }
            else
            {
                await Assert.That(error).IsNotNull();
            }
        }

        await Assert.That(complete).IsTrue();
    }
}
