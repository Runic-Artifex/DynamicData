// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the AutoRefresh class.
/// </summary>
/// <typeparam name="TObject">The type of the TObject value.</typeparam>
/// <typeparam name="TAny">The type of the TAny value.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="reEvaluator">The reEvaluator value.</param>
/// <param name="buffer">The buffer value.</param>
/// <param name="scheduler">The scheduler value.</param>
internal sealed class AutoRefresh<TObject, TAny>(IObservable<IChangeSet<TObject>> source, Func<TObject, IObservable<TAny>> reEvaluator, TimeSpan? buffer = null, IScheduler? scheduler = null)
    where TObject : notnull
{
    /// <summary>
    /// The _reEvaluator field.
    /// </summary>
    private readonly Func<TObject, IObservable<TAny>> _reEvaluator = reEvaluator ?? throw new ArgumentNullException(nameof(reEvaluator));

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly IObservable<IChangeSet<TObject>> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IChangeSet<TObject>> Run() => Observable.Create<IChangeSet<TObject>>(
            observer =>
            {
                // Source changes and refresh lookups share one delivery queue, so allItems is only
                // touched on the queue and no lock is held while the downstream observer runs.
                var queue = new SharedDeliveryQueue();

                var allItems = new List<TObject>();

                var shared = _source.SynchronizeSafe(queue).Clone(allItems) // clone all items so we can look up the index when a change has been made
                    .Publish();

                // monitor each item observable and create change. As in the cache operator, a re-evaluator
                // error ends only that item's subscription, and refreshes complete once the source has
                // completed and every item subscription has ended (completed, failed or removed).
                var itemHasChanged = shared.MergeMany(ObserveRefreshes);

                // create a change set, either buffered or one item at the time
                IObservable<IEnumerable<TObject>> itemsChanged = buffer is null ?
                    itemHasChanged.Select(t => new[] { t }) :
                    itemHasChanged.Buffer(buffer.Value, scheduler ?? GlobalConfig.DefaultScheduler).Where(list => list.Count > 0);

                IObservable<IChangeSet<TObject>> requiresRefresh = itemsChanged.SynchronizeSafe(queue).Select(
                    items => // catch all the indices of items which have been refreshed
                        allItems.IndexOfMany(items, (t, idx) => new Change<TObject>(ListChangeReason.Refresh, t, idx))).Select(changes => new ChangeSet<TObject>(changes));

                // publish refreshes and underlying changes; both inputs are serialized by the same queue
                // Deliver through the queue too: a notification raised from inside the observer, through
                // another input, is then delivered after the observer returns instead of re-entrantly.
                var publisher = shared.UnsynchronizedMerge(requiresRefresh).SubscribeSafe(queue.CreateQueue(observer));

                return new CompositeDisposable(Disposable.Create(queue.Terminate), publisher, shared.Connect());
            });

    /// <summary>
    /// Observes the re-evaluator of one item occurrence. An error from the re-evaluator ends only this
    /// item's refresh subscription, as in the cache operator; MergeMany then counts it as completed.
    /// </summary>
    /// <param name="item">The item to observe.</param>
    /// <returns>The item, each time its re-evaluator fires.</returns>
    private IObservable<TObject> ObserveRefreshes(TObject item)
    {
        // Keep factory exceptions on the source path, as before.
        var reEvaluator = _reEvaluator(item);
        ArgumentNullException.ThrowIfNull(reEvaluator);

        return Observable.Create<TObject>(observer => reEvaluator.Subscribe(
            _ => observer.OnNext(item),
            _ => observer.OnCompleted(),
            observer.OnCompleted));
    }
}
