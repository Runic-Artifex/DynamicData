#if REACTIVE_TESTS
using DynamicData.Reactive.Cache.Internal;
#else
using DynamicData.Cache.Internal;
#endif
using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

public class LockFreeObservableCacheFixture
{
    [Test]
    public async Task WatchFailsWhenTheSourceFails()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var cache = source.AsObservableCache(applyLocking: false);

        var actualError = default(Exception);
        var isCompleted = false;
        using var subscription = cache.Watch("Name1").Subscribe(static _ => { }, error => actualError = error, () => isCompleted = true);

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(actualError).IsSameReferenceAs(expectedError).Because("a watcher should be told when the source fails");
        await Assert.That(isCompleted).IsFalse().Because("the source failed, it did not complete");
    }

    [Test]
    public async Task WatchCompletesWhenTheSourceCompletes()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var cache = source.AsObservableCache(applyLocking: false);

        var actualError = default(Exception);
        var isCompleted = false;
        using var subscription = cache.Watch("Name1").Subscribe(static _ => { }, error => actualError = error, () => isCompleted = true);

        source.OnCompleted();

        await Assert.That(isCompleted).IsTrue().Because("the source completed");
        await Assert.That(actualError).IsNull().Because("no error occurred");
    }

    [Test]
    public async Task WatchCompletesWhenTheCacheIsDisposed()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        var cache = source.AsObservableCache(applyLocking: false);

        var isCompleted = false;
        using var subscription = cache.Watch("Name1").Subscribe(static _ => { }, () => isCompleted = true);

        cache.Dispose();

        await Assert.That(isCompleted).IsTrue().Because("a disposed cache can never change again");
    }

    [Test]
    public async Task CountChangedAndPreviewFailWhenTheSourceFails()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var cache = source.AsObservableCache(applyLocking: false);

        var countError = default(Exception);
        var previewError = default(Exception);
        using var countSubscription = cache.CountChanged.Subscribe(static _ => { }, error => countError = error);
        using var previewSubscription = cache.Preview().Subscribe(static _ => { }, error => previewError = error);

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(countError).IsSameReferenceAs(expectedError).Because("a count subscriber should be told when the source fails");
        await Assert.That(previewError).IsSameReferenceAs(expectedError).Because("a preview subscriber should be told when the source fails");
    }

    [Test]
    public async Task CountChangedAndPreviewCompleteWhenTheSourceCompletes()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var cache = source.AsObservableCache(applyLocking: false);

        var isCountCompleted = false;
        var isPreviewCompleted = false;
        using var countSubscription = cache.CountChanged.Subscribe(static _ => { }, () => isCountCompleted = true);
        using var previewSubscription = cache.Preview().Subscribe(static _ => { }, () => isPreviewCompleted = true);

        source.OnCompleted();

        await Assert.That(isCountCompleted).IsTrue().Because("no count change can follow the end of the source");
        await Assert.That(isPreviewCompleted).IsTrue().Because("no preview can follow the end of the source");
    }

    [Test]
    public async Task PreviewCompletesWhenACacheWithoutASourceIsDisposed()
    {
        var cache = new LockFreeObservableCache<Person, string>();

        var isCompleted = false;
        using var subscription = cache.Preview().Subscribe(static _ => { }, () => isCompleted = true);

        cache.Dispose();

        await Assert.That(isCompleted).IsTrue().Because("a disposed cache can never change again");
    }
}
