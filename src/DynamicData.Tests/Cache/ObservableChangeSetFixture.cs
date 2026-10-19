using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public class ObservableChangeSetFixture
{

    // [Test] //Disabled due to test failing when run with a test runner. Run locally in isolation and it works
    [Description("See https://github.com/reactivemarbles/DynamicData/issues/383")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Acceptable for test.")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Disabled due to test failing when run with a test runner")]
    private async Task AsyncSubscriptionCanReceiveMultipleResults()
    {

        //the aim of this test is to ensure we can continuously receive subscriptions when we use the async subscribe overloads
        var result = new List<int>();

        var observable = ObservableChangeSet.Create<int, int>(
                async (changeSet, token) =>
                {
                    var i = 0;

                    while (!token.IsCancellationRequested)
                    {
                        changeSet.AddOrUpdate(i++);

                        /*
                         *  Without ConfigureAwait(false) we get a flakey test which always work when run in isolation
                         *  but periodically fails when all tests are run. WTAF - I have no idea why but can only speculate
                         *  that without it the context is returning to the context of the test runner and it doesn't get back to it
                         *  until after the test session ends
                         */
                        await Task.Delay(5, token).ConfigureAwait(false);
                    }
                },
                i => i)
            .Select(cs => cs.Select(c => c.Current).ToList());

        var isComplete = false;
        Exception? error = null;

        //load list of results
        var subscriber = observable
            .Subscribe(item => result.AddRange(item), ex => error = ex, () => isComplete = true);

        //allow some results through
        await Task.Delay(100);

        await Assert.That(isComplete).IsFalse();
        await Assert.That(error).IsNull();

        //do not try to be clever with timings because wierd stuff happens in time
        await Assert.That(result.Take(5)).IsEquivalentTo(new List<int>
        {
            0,
            1,
            2,
            3,
            4
        });

        subscriber.Dispose();
    }

    [Test]
    public async Task HandlesAsyncError()
    {
        Exception? error = null;

        static Task<IEnumerable<Person>> Loader() => throw new Exception("Broken");

        var observable = ObservableChangeSet.Create<Person, string>(
            async cache =>
            {
                var people = await Loader();
                cache.AddOrUpdate(people);
                return () => { };
            },
            p => p.Name);

        using var dervived = observable.AsObservableCache();
        using (dervived.Connect().Subscribe(_ => { }, ex => error = ex))
        {
            await Assert.That(error).IsNotNull();
        }
    }

    [Test]
    public async Task HandlesError()
    {
        Exception? error = null;

        static IEnumerable<Person> Loader() => throw new Exception("Broken");

        var observable = ObservableChangeSet.Create<Person, string>(
            cache =>
            {
                var people = Loader();
                cache.AddOrUpdate(people);
                return () => { };
            },
            p => p.Name);

        using var derived = observable.AsObservableCache();
        using (derived.Connect().Subscribe(_ => { }, ex => error = ex))
        {
            await Assert.That(error).IsNotNull();
        }
    }

    [Test]
    public async Task LoadsAndDisposeFromObservableCache()
    {
        var isDisposed = false;

        var observable = ObservableChangeSet.Create<Person, string>(cache => () => isDisposed = true, p => p.Name);

        observable.AsObservableCache().Dispose();
        await Assert.That(isDisposed).IsTrue();
    }

    [Test]
    public async Task LoadsAndDisposeUsingAction()
    {
        var isDisposed = false;
        await SubscribeAndAssert(
            ObservableChangeSet.Create<Person, string>(
                cache =>
                {
                    var people = Enumerable.Range(1, 100).Select(i => new Person($"Name.{i}", i)).ToArray();
                    cache.AddOrUpdate(people);
                    return () => isDisposed = true;
                },
                p => p.Name),
checkContentAction: async result => { await Assert.That(result.Count).IsEqualTo(100); });

        await Assert.That(isDisposed).IsTrue();
    }

    [Test]
    public async Task LoadsAndDisposeUsingActionAsync()
    {
        static Task<Person[]> CreateTask() => Task.FromResult(Enumerable.Range(1, 100).Select(i => new Person($"Name.{i}", i)).ToArray());

        var isDisposed = false;
        await SubscribeAndAssert(
            ObservableChangeSet.Create<Person, string>(
                async cache =>
                {
                    var people = await CreateTask();
                    cache.AddOrUpdate(people);
                    return () => isDisposed = true;
                },
                p => p.Name),
checkContentAction: async result => { await Assert.That(result.Count).IsEqualTo(100); });

        await Assert.That(isDisposed).IsTrue();
    }

    [Test]
    public async Task LoadsAndDisposeUsingDisposable()
    {
        var isDisposed = false;
        await SubscribeAndAssert(
            ObservableChangeSet.Create<Person, string>(
                cache =>
                {
                    var people = Enumerable.Range(1, 100).Select(i => new Person($"Name.{i}", i)).ToArray();
                    cache.AddOrUpdate(people);
                    return Disposable.Create(() => isDisposed = true);
                },
                p => p.Name),
checkContentAction: async result => { await Assert.That(result.Count).IsEqualTo(100); });

        await Assert.That(isDisposed).IsTrue();
    }

    [Test]
    public async Task LoadsAndDisposeUsingDisposableAsync()
    {
        static Task<Person[]> CreateTask() => Task.FromResult(Enumerable.Range(1, 100).Select(i => new Person($"Name.{i}", i)).ToArray());

        var isDisposed = false;
        await SubscribeAndAssert(
            ObservableChangeSet.Create<Person, string>(
                async cache =>
                {
                    var people = await CreateTask();
                    cache.AddOrUpdate(people);
                    return Disposable.Create(() => isDisposed = true);
                },
                p => p.Name),
checkContentAction: async result => { await Assert.That(result.Count).IsEqualTo(100); });

        await Assert.That(isDisposed).IsTrue();
    }

    [Test]
    public async Task TaskCompletesTheStreamAfterItsChanges()
    {
        var task = new TaskCompletionSource();
        ISourceCache<Person, string>? captured = null;
        var person = new Person("Name1", 10);

        using var subscription = ObservableChangeSet.Create<Person, string>((cache, _) => { captured = cache; return task.Task; }, p => p.Name).RecordCacheItems(out var results);

        captured!.AddOrUpdate(person);
        await Assert.That(results.HasCompleted).IsFalse().Because("the task has not completed yet");

        task.SetResult();

        await Assert.That(results.RecordedItemsByKey.Values.ToArray()).IsEquivalentTo(new[] { person }).Because("changes made by the task arrive ahead of completion");
        await Assert.That(results.HasCompleted).IsTrue().Because("nothing can follow the end of the task");
        await Assert.That(results.Error).IsNull();
    }

    [Test]
    public async Task TaskFailureIsDeliveredOnTheThreadThatFailsIt()
    {
        var task = new TaskCompletionSource();

        using var subscription = ObservableChangeSet.Create<Person, string>((_, _) => task.Task, p => p.Name).RecordCacheItems(out var results);

        var expectedError = new Exception("Test Exception");
        task.SetException(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the failure should arrive before SetException returns, not later on another thread");
    }

    [Test]
    public async Task TaskOutcomeIsNotPostedToTheCompletingThreadsSynchronizationContext()
    {
        var task = new TaskCompletionSource();
        var context = new RecordingSynchronizationContext();
        var completedBeforeSetResultReturned = false;

        using var subscription = ObservableChangeSet.Create<Person, string>((_, _) => task.Task, p => p.Name).RecordCacheItems(out var results);

        var thread = new Thread(() =>
        {
            SynchronizationContext.SetSynchronizationContext(context);
            task.SetResult();
            completedBeforeSetResultReturned = results.HasCompleted;
        });
        thread.Start();
        thread.Join();

        await Assert.That(completedBeforeSetResultReturned).IsTrue().Because("completion arrives on the thread that completes the task");
        await Assert.That(context.Posts).IsEqualTo(0);
    }

    [Test]
    public async Task TaskCompletingWhileAChangeIsBeingDeliveredCompletesAfterIt()
    {
        var task = new TaskCompletionSource();
        ISourceCache<Person, string>? captured = null;
        var completerReturned = false;
        var observer = new OverlapDetectingObserver<IChangeSet<Person, string>>(_ =>
        {
            // Complete the task on another thread while this change is still being delivered.
            var completer = new Thread(() => task.SetResult());
            completer.Start();
            completerReturned = completer.Join(TimeSpan.FromSeconds(10));
        });

        using var subscription = ObservableChangeSet.Create<Person, string>((cache, _) => { captured = cache; return task.Task; }, p => p.Name).Subscribe(observer);
        captured!.AddOrUpdate(new Person("Name0", 0));

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
            ISourceCache<Person, string>? captured = null;
            var observer = new OverlapDetectingObserver<IChangeSet<Person, string>>(_ => Thread.SpinWait(200));
            using var subscription = ObservableChangeSet.Create<Person, string>((cache, _) => { captured = cache; return task.Task; }, p => p.Name).Subscribe(observer);

            var stop = 0;
            var editor = new Thread(() =>
            {
                try
                {
                    for (var i = 0; Volatile.Read(ref stop) == 0; i++)
                    {
                        captured!.AddOrUpdate(new Person($"Name{i}", i));
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
    public async Task DisposingTheSubscriptionCancelsTheTask()
    {
        var token = CancellationToken.None;

        var subscription = ObservableChangeSet.Create<Person, string>((_, cancellationToken) => { token = cancellationToken; return new TaskCompletionSource().Task; }, p => p.Name).RecordCacheItems(out _);
        subscription.Dispose();

        await Assert.That(token.IsCancellationRequested).IsTrue();
    }

    [Test]
    public async Task TaskThatFinishesDuringSubscriptionStillDeliversItsChanges()
    {
        var person = new Person("Name1", 10);

        using var subscription = ObservableChangeSet.Create<Person, string>((cache, _) => { cache.AddOrUpdate(person); return Task.CompletedTask; }, p => p.Name).RecordCacheItems(out var results);

        await Assert.That(results.RecordedItemsByKey.Values.ToArray()).IsEquivalentTo(new[] { person }).Because("the change was made before the task finished");
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Error).IsNull();
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Accetable for test.")]
    private async Task SubscribeAndAssert<TObject, TKey>(IObservable<IChangeSet<TObject, TKey>> observableChangeset, bool expectsError = false, Func<IObservableCache<TObject, TKey>, Task>? checkContentAction = null)
        where TKey : notnull
        where TObject : notnull
    {
        Exception? error = null;
        var complete = false;
        IChangeSet<TObject, TKey>? changes = null;

        using (var cache = observableChangeset.Finally(() => complete = true).AsObservableCache())
        using (cache.Connect().Subscribe(result => changes = result, ex => error = ex))
        {
            if (!expectsError)
            {
                await Assert.That(error).IsNull();
            }
            else
            {
                await Assert.That(error).IsNotNull();
            }

            if (checkContentAction is not null) { await checkContentAction(cache); }
        }

        await Assert.That(complete).IsTrue();
    }

    private sealed class RecordingSynchronizationContext : SynchronizationContext
    {
        public int Posts { get; private set; }

        public override void Post(SendOrPostCallback d, object? state)
        {
            Posts++;
            base.Post(d, state);
        }
    }
}
