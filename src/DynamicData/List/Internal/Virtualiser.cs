// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the Virtualiser class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="requests">The requests value.</param>
internal sealed class Virtualiser<T>(IObservable<IChangeSet<T>> source, IObservable<IVirtualRequest> requests)
    where T : notnull
{
    /// <summary>
    /// The _requests field.
    /// </summary>
    private readonly IObservable<IVirtualRequest> _requests = requests ?? throw new ArgumentNullException(nameof(requests));

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly IObservable<IChangeSet<T>> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IVirtualChangeSet<T>> Run() => Observable.Create<IVirtualChangeSet<T>>(
        observer =>
        {
            var queue = new SharedDeliveryQueue();
            var all = new List<Occurrence>();
            var virtualised = new ChangeAwareList<Occurrence>();
            IVirtualRequest parameters = VirtualRequest.Default;
            IVirtualResponse previousResponse = new VirtualResponse(parameters.Size, parameters.StartIndex, 0);

            var requestStream = _requests.SynchronizeSafe(queue)
                .Where(request => request is { StartIndex: >= 0, Size: >= 0 })
                .DistinctUntilChanged(VirtualRequest.StartIndexSizeComparer)
                .Select(request =>
                {
                    // Invalid requests never replace the last valid window.
                    parameters = request;
                    return Virtualise(all, virtualised, parameters);
                });

            // Each addition owns a token, including equal values and repeated references.
            var dataChanged = _source.SynchronizeSafe(queue)
                .Transform(item => new Occurrence(item))
                .Select(changes => Virtualise(all, virtualised, parameters, changes));

            var publisher = requestStream.UnsynchronizedMerge(dataChanged)
                .Select(changes =>
                {
                    var response = new VirtualResponse(parameters.Size, parameters.StartIndex, all.Count);
                    var result = changes.Count != 0 || !response.Equals(previousResponse)
                        ? new VirtualChangeSet<T>(changes.Transform(occurrence => occurrence.Item), response)
                        : null;
                    previousResponse = response;
                    return result;
                })
                .Where(changes => changes is not null)
                .Select(changes => changes!)
                .SubscribeSafe(observer);

            return new CompositeDisposable(publisher, queue);
        });

    private static IChangeSet<Occurrence> Virtualise(List<Occurrence> all,
        ChangeAwareList<Occurrence> virtualised, IVirtualRequest request, IChangeSet<Occurrence>? changeSet = null)
    {
        if (changeSet is not null)
        {
            all.Clone(changeSet);
        }

        var current = all.Skip(request.StartIndex).Take(request.Size).ToList();
        var currentSet = new HashSet<Occurrence>(current);

        // Remove by index so duplicate values cannot select the wrong occurrence.
        for (var index = virtualised.Count - 1; index >= 0; index--)
        {
            if (!currentSet.Contains(virtualised[index])) virtualised.RemoveAt(index);
        }

        // Keep a single in-window source move as one downstream move.
        if (changeSet is { Count: 1 } && changeSet.First().Reason == ListChangeReason.Moved)
        {
            var moved = changeSet.First().Item.Current;
            var previousIndex = virtualised.IndexOf(moved);
            var currentIndex = current.IndexOf(moved);
            if (previousIndex >= 0 && currentIndex >= 0 && previousIndex != currentIndex)
            {
                virtualised.Move(previousIndex, currentIndex);
            }
        }

        for (var index = 0; index < current.Count; index++)
        {
            var occurrence = current[index];
            if (index < virtualised.Count && ReferenceEquals(virtualised[index], occurrence)) continue;

            var previousIndex = virtualised.IndexOf(occurrence);
            if (previousIndex >= 0) virtualised.Move(previousIndex, index);
            else virtualised.Insert(index, occurrence);
        }

        if (changeSet is not null)
        {
            foreach (var change in changeSet.Where(change => change.Reason == ListChangeReason.Refresh))
            {
                var index = current.IndexOf(change.Item.Current);
                if (index >= 0) virtualised.RefreshAt(index);
            }
        }

        return virtualised.CaptureChanges();
    }

    private sealed class Occurrence(T item)
    {
        public T Item { get; } = item;
    }
}
