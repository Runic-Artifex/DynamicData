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
