// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Aggregation;
#else

namespace DynamicData.Aggregation;
#endif

/// <summary>
/// Shared accumulation for aggregates whose removals must use the projection that was added.
/// </summary>
internal static class ProjectedAggregation
{
    /// <summary>
    /// Accumulates aggregate changes into a state, one state per aggregate batch.
    /// </summary>
    /// <typeparam name="TObject">The type of the aggregated item.</typeparam>
    /// <typeparam name="TValue">The type of the projected value.</typeparam>
    /// <typeparam name="TState">The type of the accumulator state.</typeparam>
    /// <param name="source">The aggregate change stream.</param>
    /// <param name="valueSelector">The projection of an item.</param>
    /// <param name="addAction">Adds a projected value to the state.</param>
    /// <param name="removeAction">Removes a projected value from the state.</param>
    /// <param name="isEmpty">Whether the state holds no values; empty states are reset to default.</param>
    /// <param name="retainProjections">
    /// <see langword="true"/> to retain each occurrence's projection so that removals subtract
    /// the stored value and an empty batch re-evaluates every occurrence. <see langword="false"/>
    /// when items are already immutable projections (direct overloads after Transform).
    /// </param>
    /// <returns>An observable of accumulated states.</returns>
    public static IObservable<TState> ScanProjected<TObject, TValue, TState>(this IObservable<IAggregateChangeSet<TObject>> source, Func<TObject, TValue> valueSelector, Func<TState, TValue, TState> addAction, Func<TState, TValue, TState> removeAction, Func<TState, bool> isEmpty, bool retainProjections)
        where TState : struct
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        ArgumentNullException.ThrowIfNull(addAction);
        ArgumentNullException.ThrowIfNull(removeAction);
        ArgumentNullException.ThrowIfNull(isEmpty);

        return Observable.Defer(() =>
        {
            var projections = new List<(TObject Item, TValue Value)>();
            return source.Scan(default(TState), (state, changes) =>
            {
                var hasMembershipChanges = false;
                var mutations = retainProjections ? changes : WithoutUnchangedPairs(changes, valueSelector);
                using var iterator = mutations.GetEnumerator();
                var hasCurrent = iterator.MoveNext();
                while (hasCurrent)
                {
                    var change = iterator.Current;
                    hasCurrent = iterator.MoveNext();
                    hasMembershipChanges = true;
                    if (retainProjections && change.Type == AggregateType.Remove && hasCurrent && iterator.Current.Type == AggregateType.Add)
                    {
                        var index = OccurrenceIndex(projections, change.Item);
                        if (index < 0)
                        {
                            throw new UnspecifiedIndexException($"Cannot find aggregate occurrence of {change.Item}");
                        }

                        var replacement = iterator.Current;
                        var previousValue = projections[index].Value;
                        var currentValue = valueSelector(replacement.Item);
                        projections.RemoveAt(index);
                        projections.Add((replacement.Item, currentValue));
                        if (!EqualityComparer<TValue>.Default.Equals(previousValue, currentValue))
                        {
                            state = removeAction(state, previousValue);
                            state = addAction(state, currentValue);
                        }

                        hasCurrent = iterator.MoveNext();
                        continue;
                    }

                    if (change.Type == AggregateType.Add)
                    {
                        var value = valueSelector(change.Item);
                        if (retainProjections)
                        {
                            projections.Add((change.Item, value));
                        }

                        state = addAction(state, value);
                    }
                    else if (retainProjections)
                    {
                        var index = OccurrenceIndex(projections, change.Item);
                        if (index < 0)
                        {
                            throw new UnspecifiedIndexException($"Cannot find aggregate occurrence of {change.Item}");
                        }

                        state = removeAction(state, projections[index].Value);
                        projections.RemoveAt(index);
                    }
                    else
                    {
                        state = removeAction(state, valueSelector(change.Item));
                    }
                }

                // ForAggregation drops Refresh and Moved details. Only an empty aggregate
                // batch can serve as an invalidation signal; it invalidates every occurrence.
                if (retainProjections && !hasMembershipChanges)
                {
                    for (var index = 0; index < projections.Count; index++)
                    {
                        var item = projections[index].Item;
                        var value = valueSelector(item);
                        if (EqualityComparer<TValue>.Default.Equals(projections[index].Value, value))
                        {
                            continue;
                        }

                        state = removeAction(state, projections[index].Value);
                        state = addAction(state, value);
                        projections[index] = (item, value);
                    }
                }

                if (isEmpty(state))
                {
                    state = default;
                }

                return state;
            });
        });
    }

    private static int OccurrenceIndex<TObject, TValue>(List<(TObject Item, TValue Value)> projections, TObject item)
    {
        if (!typeof(TObject).IsValueType)
        {
            for (var index = 0; index < projections.Count; index++)
            {
                if (ReferenceEquals(projections[index].Item, item))
                {
                    return index;
                }
            }
        }

        for (var index = 0; index < projections.Count; index++)
        {
            if (EqualityComparer<TObject>.Default.Equals(projections[index].Item, item))
            {
                return index;
            }
        }

        return -1;
    }

    private static IEnumerable<AggregateItem<TObject>> WithoutUnchangedPairs<TObject, TValue>(IEnumerable<AggregateItem<TObject>> changes, Func<TObject, TValue> valueSelector)
    {
        using var iterator = changes.GetEnumerator();
        var hasCurrent = iterator.MoveNext();
        while (hasCurrent)
        {
            var current = iterator.Current;
            hasCurrent = iterator.MoveNext();
            // Transform emits Remove/Add for a Replace or Update. Avoid perturbing
            // floating-point accumulators (including infinity) when the projection is unchanged.
            if (current.Type == AggregateType.Remove && hasCurrent && iterator.Current.Type == AggregateType.Add &&
                EqualityComparer<TValue>.Default.Equals(valueSelector(current.Item), valueSelector(iterator.Current.Item)))
            {
                hasCurrent = iterator.MoveNext();
                continue;
            }

            yield return current;
        }
    }
}
