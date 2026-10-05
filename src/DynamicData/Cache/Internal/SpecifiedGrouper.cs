// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Cache.Internal;
#else

namespace DynamicData.Cache.Internal;
#endif

/// <summary>
/// Provides members for the SpecifiedGrouper class.
/// </summary>
/// <typeparam name="TObject">The type of the TObject value.</typeparam>
/// <typeparam name="TKey">The type of the TKey value.</typeparam>
/// <typeparam name="TGroupKey">The type of the TGroupKey value.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="groupSelector">The groupSelector value.</param>
/// <param name="resultGroupSource">The resultGroupSource value.</param>
internal sealed class SpecifiedGrouper<TObject, TKey, TGroupKey>(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TGroupKey> groupSelector, IObservable<IDistinctChangeSet<TGroupKey>> resultGroupSource)
    where TObject : notnull
    where TKey : notnull
    where TGroupKey : notnull
{
    /// <summary>
    /// The _groupSelector field.
    /// </summary>
    private readonly Func<TObject, TGroupKey> _groupSelector = groupSelector ?? throw new ArgumentNullException(nameof(groupSelector));

    /// <summary>
    /// The _resultGroupSource field.
    /// </summary>
    private readonly IObservable<IDistinctChangeSet<TGroupKey>> _resultGroupSource = resultGroupSource ?? throw new ArgumentNullException(nameof(resultGroupSource));

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly IObservable<IChangeSet<TObject, TKey>> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IGroupChangeSet<TObject, TKey, TGroupKey>> Run() => Observable.Create<IGroupChangeSet<TObject, TKey, TGroupKey>>(
            observer =>
            {
                var queue = new SharedDeliveryQueue();

                // Both caches subscribe eagerly. Connect their inputs only after the notifier and child
                // subscriptions are wired, then release a synchronous terminal after both initial snapshots.
                var initializing = true;
                var completed = false;
                void Fail(Exception failure) => observer.OnError(failure);
                void Finish()
                {
                    if (initializing) completed = true;
                    else observer.OnCompleted();
                }

                var source = _source.SynchronizeSafe(queue).Publish();
                var groups = _resultGroupSource.SynchronizeSafe(queue).Publish();
                var sourceGroups = source.Do(static _ => { }, Fail, Finish).Group(_groupSelector).DisposeMany().AsObservableCache();

                // create parent groups
                var parentGroups = groups.Transform(
                    x =>
                    {
                        // if child already has data, populate it.
                        var result = new ManagedGroup<TObject, TKey, TGroupKey>(x);
                        var child = sourceGroups.Lookup(x);
                        if (child.HasValue)
                        {
                            // dodgy cast but fine as a groups is always a ManagedGroup;
                            var group = (ManagedGroup<TObject, TKey, TGroupKey>)child.Value;
                            result.Update(updater => updater.Clone(group.GetInitialUpdates()));
                        }

                        return result;
                    }).DisposeMany().AsObservableCache();

                // connect to each individual item and update the resulting group
                var updatesFromChildren = sourceGroups.Connect().SubscribeMany(
                    x => x.Cache.Connect().Subscribe(
                        updates =>
                        {
                            var groupToUpdate = parentGroups.Lookup(x.Key);
                            if (groupToUpdate.HasValue)
                            {
                                groupToUpdate.Value.Update(updater => updater.Clone(updates));
                            }
                        })).DisposeMany().Subscribe(static _ => { }, static _ => { });

                var notifier = parentGroups.Connect().Select(
                    x =>
                    {
                        var groups = x.Select(s => new Change<IGroup<TObject, TKey, TGroupKey>, TGroupKey>(s.Reason, s.Key, s.Current));
                        return new GroupChangeSet<TObject, TKey, TGroupKey>(groups);
                    }).Subscribe(observer.OnNext, Fail, Finish);

                var sourceConnection = source.Connect();
                var groupConnection = groups.Connect();
                var initialization = queue.CreateQueue(Observer.Create<Unit>(_ =>
                {
                    initializing = false;
                    if (completed) observer.OnCompleted();
                }));
                initialization.OnNext(Unit.Default);

                return new CompositeDisposable(sourceConnection, groupConnection, notifier, sourceGroups, parentGroups, updatesFromChildren, initialization, queue);
            });
}
