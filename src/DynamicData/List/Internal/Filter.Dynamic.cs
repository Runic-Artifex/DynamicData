// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the Filter class.
/// </summary>
internal static partial class Filter
{
    /// <summary>
    /// Provides members for the Dynamic class.
    /// </summary>
    /// <typeparam name="T">The type of the T value.</typeparam>
    internal sealed class Dynamic<T>
            where T : notnull
    {
        /// <summary>
        /// The _policy field.
        /// </summary>
        private readonly ListFilterPolicy _policy;

        /// <summary>
        /// The _predicate field.
        /// </summary>
        private readonly Func<T, bool>? _predicate;

        /// <summary>
        /// The _predicates field.
        /// </summary>
        private readonly IObservable<Func<T, bool>>? _predicates;

        /// <summary>
        /// The _source field.
        /// </summary>
        private readonly IObservable<IChangeSet<T>> _source;

        /// <summary>
        /// Initializes a new instance of the <see cref="Dynamic{T}"/> class.
        /// </summary>
        /// <param name="source">The source value.</param>
        /// <param name="predicates">The predicates value.</param>
        /// <param name="policy">The policy value.</param>
        public Dynamic(IObservable<IChangeSet<T>> source, IObservable<Func<T, bool>> predicates, ListFilterPolicy policy = ListFilterPolicy.CalculateDiff)
        {
            _policy = policy;
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _predicates = predicates ?? throw new ArgumentNullException(nameof(predicates));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Dynamic{T}"/> class.
        /// </summary>
        /// <param name="source">The source value.</param>
        /// <param name="predicate">The predicate value.</param>
        /// <param name="policy">The policy value.</param>
        public Dynamic(IObservable<IChangeSet<T>> source, Func<T, bool> predicate, ListFilterPolicy policy = ListFilterPolicy.CalculateDiff)
        {
            _policy = policy;
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        }

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result of the operation.</returns>
        public IObservable<IChangeSet<T>> Run() => Observable.Create<IChangeSet<T>>(
                observer =>
                {
                    var queue = new SharedDeliveryQueue();

                    Func<T, bool> predicate = _ => false;
                    var all = new List<ItemWithMatch>();
                    var filtered = new ChangeAwareList<ItemWithMatch>();
                    var immutableFilter = _predicate is not null;

                    IObservable<IChangeSet<ItemWithMatch>> predicateChanged;

                    if (immutableFilter)
                    {
                        predicateChanged = Observable.Never<IChangeSet<ItemWithMatch>>();
                        predicate = _predicate ?? predicate;
                    }
                    else
                    {
                        if (_predicates is null)
                        {
                            throw new InvalidOperationException("The predicates is not set and the change is not a immutableFilter.");
                        }

                        predicateChanged = _predicates.SynchronizeSafe(queue).Select(
                            newPredicate =>
                            {
                                predicate = newPredicate;
                                return Requery(predicate, all, filtered);
                            });
                    }

                    /*
                     * Apply the transform operator so 'IsMatch' state can be evaluated and captured one time only
                     * This is to eliminate the need to re-apply the predicate when determining whether an item was previously matched,
                     * which is essential when we have mutable state
                     */

                    // Need to get item by index and store it in the transform
                    var filteredResult = _source.SynchronizeSafe(queue).Transform<T, ItemWithMatch>(
                        (t, previous) =>
                            {
                                var wasMatch = previous.ConvertOr(p => p!.IsMatch, () => false);
                                return new ItemWithMatch(t, predicate(t), wasMatch);
                            },
                        false)
                        .Select(changes => Process(predicate, all, filtered, changes));

                    var publisher = predicateChanged.UnsynchronizedMerge(filteredResult).NotEmpty()
                        .Select(changes => changes.Transform(iwm => iwm.Item)) // use convert, not transform
                        .SubscribeSafe(observer);
                    return new CompositeDisposable(publisher, queue);
                });

        /// <summary>
        /// Executes the Process operation.
        /// </summary>
        /// <param name="predicate">The active predicate.</param>
        /// <param name="all">The upstream occurrence states.</param>
        /// <param name="filtered">The filtered value.</param>
        /// <param name="changes">The changes value.</param>
        /// <returns>The result of the operation.</returns>
        private static IChangeSet<ItemWithMatch> Process(Func<T, bool> predicate, List<ItemWithMatch> all, ChangeAwareList<ItemWithMatch> filtered, IChangeSet<ItemWithMatch> changes)
        {
            // Keep occurrence identities and upstream indexes throughout the batch, including equal-valued swaps.
            foreach (var item in changes)
            {
                switch (item.Reason)
                {
                    case ListChangeReason.Add:
                        {
                            var change = item.Item;
                            var index = change.CurrentIndex < 0 ? all.Count : change.CurrentIndex;
                            all.Insert(index, change.Current);
                            if (change.Current.IsMatch)
                            {
                                filtered.Insert(CountMatchingBefore(all, index), change.Current);
                            }

                            break;
                        }

                    case ListChangeReason.AddRange:
                        {
                            var index = item.Range.Index < 0 ? all.Count : item.Range.Index;
                            var downstreamIndex = CountMatchingBefore(all, index);
                            all.InsertRange(index, item.Range);
                            filtered.InsertRange(item.Range.Where(static state => state.IsMatch).ToList(), downstreamIndex);
                            break;
                        }

                    case ListChangeReason.Replace:
                        {
                            var change = item.Item;
                            var previous = change.Previous.Value;
                            var previousIndex = change.PreviousIndex >= 0 ? change.PreviousIndex
                                : change.CurrentIndex >= 0 ? change.CurrentIndex : FindStateIndex(all, previous);
                            var currentIndex = change.CurrentIndex < 0 ? previousIndex : change.CurrentIndex;
                            var downstreamPreviousIndex = previous.IsMatch ? FindStateIndex(filtered, previous) : -1;
                            all.RemoveAt(previousIndex);
                            all.Insert(currentIndex, change.Current);
                            if (previous.IsMatch && change.Current.IsMatch && previousIndex == currentIndex)
                            {
                                filtered[downstreamPreviousIndex] = change.Current;
                            }
                            else
                            {
                                if (previous.IsMatch)
                                {
                                    filtered.RemoveAt(downstreamPreviousIndex);
                                }

                                if (change.Current.IsMatch)
                                {
                                    filtered.Insert(CountMatchingBefore(all, currentIndex), change.Current);
                                }
                            }

                            break;
                        }

                    case ListChangeReason.Refresh:
                        {
                            var change = item.Item;
                            var state = change.Current;
                            var wasMatch = state.IsMatch;
                            state.WasMatch = wasMatch;
                            state.IsMatch = predicate(state.Item);
                            if (wasMatch && state.IsMatch)
                            {
                                filtered.RefreshAt(FindStateIndex(filtered, state));
                            }
                            else if (wasMatch)
                            {
                                filtered.RemoveAt(FindStateIndex(filtered, state));
                            }
                            else if (state.IsMatch)
                            {
                                var index = change.CurrentIndex < 0 ? FindStateIndex(all, state) : change.CurrentIndex;
                                filtered.Insert(CountMatchingBefore(all, index), state);
                            }

                            break;
                        }

                    case ListChangeReason.Moved:
                        {
                            var change = item.Item;
                            var state = all[change.PreviousIndex];
                            var previousRank = state.IsMatch ? CountMatchingBefore(all, change.PreviousIndex) : -1;
                            all.RemoveAt(change.PreviousIndex);
                            all.Insert(change.CurrentIndex, state);
                            if (state.IsMatch)
                            {
                                var currentIndex = CountMatchingBefore(all, change.CurrentIndex);
                                // CalculateDiff may retain a historical requery order. A source no-op,
                                // or crossing only excluded rows, must not rearrange that order.
                                if (previousRank != currentIndex)
                                {
                                    var previousIndex = FindStateIndex(filtered, state);
                                    if (previousIndex != currentIndex)
                                    {
                                        filtered.Move(previousIndex, currentIndex);
                                    }
                                }
                            }

                            break;
                        }

                    case ListChangeReason.Remove:
                        {
                            var state = item.Item.Current;
                            var index = item.Item.CurrentIndex < 0 ? FindStateIndex(all, state) : item.Item.CurrentIndex;
                            all.RemoveAt(index);
                            if (state.IsMatch)
                            {
                                filtered.RemoveAt(FindStateIndex(filtered, state));
                            }

                            break;
                        }

                    case ListChangeReason.RemoveRange:
                        {
                            foreach (var state in item.Range)
                            {
                                all.RemoveAt(FindStateIndex(all, state));
                                if (state.IsMatch)
                                {
                                    filtered.RemoveAt(FindStateIndex(filtered, state));
                                }
                            }

                            break;
                        }

                    case ListChangeReason.Clear:
                        all.Clear();
                        filtered.Clear();
                        break;
                }
            }

            return filtered.CaptureChanges();
        }

        private static int CountMatchingBefore(List<ItemWithMatch> items, int index)
        {
            var count = 0;
            for (var i = 0; i < index; i++)
            {
                if (items[i].IsMatch)
                {
                    count++;
                }
            }

            return count;
        }

        private static int FindStateIndex(IList<ItemWithMatch> items, ItemWithMatch state)
        {
            for (var i = 0; i < items.Count; i++)
            {
                if (ReferenceEquals(items[i], state))
                {
                    return i;
                }
            }

            throw new InvalidOperationException($"Cannot find filtered occurrence of {typeof(T).Name} -> {state.Item}");
        }

        /// <summary>
        /// Executes the Requery operation.
        /// </summary>
        /// <param name="predicate">The predicate value.</param>
        /// <param name="all">The all value.</param>
        /// <param name="filtered">The filtered value.</param>
        /// <returns>The result of the operation.</returns>
        private IChangeSet<ItemWithMatch> Requery(Func<T, bool> predicate, List<ItemWithMatch> all, ChangeAwareList<ItemWithMatch> filtered)
        {
            if (all.Count == 0)
            {
                return ChangeSet<ItemWithMatch>.Empty;
            }

            if (_policy == ListFilterPolicy.ClearAndReplace)
            {
                foreach (var state in all)
                {
                    state.WasMatch = state.IsMatch;
                    state.IsMatch = predicate(state.Item);
                }

                filtered.Clear();
                filtered.AddRange(all.Where(static state => state.IsMatch));
                return filtered.CaptureChanges();
            }

            var toAdd = new List<ItemWithMatch>(all.Count);
            var toRemove = new List<ItemWithMatch>(all.Count);

            foreach (var state in all)
            {
                state.WasMatch = state.IsMatch;
                state.IsMatch = predicate(state.Item);
                if (state.IsMatch && !state.WasMatch)
                {
                    toAdd.Add(state);
                }
                else if (!state.IsMatch && state.WasMatch)
                {
                    toRemove.Add(state);
                }
            }

            foreach (var state in toRemove)
            {
                filtered.RemoveAt(FindStateIndex(filtered, state));
            }

            filtered.AddRange(toAdd);

            return filtered.CaptureChanges();
        }

        /// <summary>
        /// Provides members for the ItemWithMatch class.
        /// </summary>
        /// <param name="item">The item value.</param>
        /// <param name="isMatch">The isMatch value.</param>
        /// <param name="wasMatch">The wasMatch value.</param>
        private sealed class ItemWithMatch(T item, bool isMatch, bool wasMatch = false) : IEquatable<ItemWithMatch>
        {
            /// <summary>
            /// Gets the Item value.
            /// </summary>
            public T Item { get; } = item;

            /// <summary>
            /// Gets or sets the IsMatch value.
            /// </summary>
            public bool IsMatch { get; set; } = isMatch;

            /// <summary>
            /// Gets or sets the WasMatch value.
            /// </summary>
            public bool WasMatch { get; set; } = wasMatch;

            /// <summary>
            /// Executes the operator operation.
            /// </summary>
            /// <param name="left">The left value.</param>
            /// <param name="right">The right value.</param>
            /// <returns>The result of the operation.</returns>
            public static bool operator ==(ItemWithMatch? left, ItemWithMatch? right) =>
                Equals(left, right);

            /// <summary>
            /// Executes the operator operation.
            /// </summary>
            /// <param name="left">The left value.</param>
            /// <param name="right">The right value.</param>
            /// <returns>The result of the operation.</returns>
            public static bool operator !=(ItemWithMatch? left, ItemWithMatch? right) =>
                !Equals(left, right);

            /// <summary>
            /// Executes the Equals operation.
            /// </summary>
            /// <param name="other">The other value.</param>
            /// <returns>The result of the operation.</returns>
            public bool Equals(ItemWithMatch? other)
            {
                if (other is null)
                {
                    return false;
                }

                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                return EqualityComparer<T>.Default.Equals(Item, other.Item);
            }

            /// <summary>
            /// Executes the Equals operation.
            /// </summary>
            /// <param name="obj">The obj value.</param>
            /// <returns>The result of the operation.</returns>
            public override bool Equals(object? obj)
            {
                if (obj is null)
                {
                    return false;
                }

                if (ReferenceEquals(this, obj))
                {
                    return true;
                }

                if (obj.GetType() != GetType())
                {
                    return false;
                }

                return Equals((ItemWithMatch)obj);
            }

            /// <summary>
            /// Executes the GetHashCode operation.
            /// </summary>
            /// <returns>The result of the operation.</returns>
            public override int GetHashCode() => EqualityComparer<T>.Default.GetHashCode(Item!);

            /// <summary>
            /// Executes the ToString operation.
            /// </summary>
            /// <returns>The result of the operation.</returns>
            public override string ToString() => $"{Item}, (was {IsMatch} is {WasMatch}";
        }
    }
}
