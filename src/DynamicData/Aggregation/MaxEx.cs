// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Aggregation;
#else

namespace DynamicData.Aggregation;
#endif

/// <summary>
/// Maximum and minimum value extensions.
/// </summary>
/// <remarks>
/// Refresh, replacement and removal batches recompute the extremum from the current collection in O(n) time.
/// Pure additions update the cached extremum incrementally. Unchanged results are suppressed.
/// </remarks>
public static class MaxEx
{
/// <summary>
/// Defines values for the MaxOrMin enumeration.
/// </summary>
private enum MaxOrMin
    {
        /// <summary>
        /// The Max value.
        /// </summary>
        Max,

        /// <summary>
        /// The Min value.
        /// </summary>
        Min
    }

    /// <summary>
    /// Continually calculates the maximum value from the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The value to use when the underlying collection is empty.</param>
    /// <returns>
    /// A distinct observable of the maximum item.
    /// </returns>
    public static IObservable<TResult> Maximum<TObject, TResult>(this IObservable<IChangeSet<TObject>> source, Func<TObject, TResult> valueSelector, TResult emptyValue = default)
        where TObject : notnull
        where TResult : struct, IComparable<TResult>
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);

        return source.ToChangesAndCollection().Calculate(valueSelector, MaxOrMin.Max, emptyValue);
    }

    /// <summary>
    /// Continually calculates the maximum value from the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The value to use when the underlying collection is empty.</param>
    /// <returns>
    /// A distinct observable of the maximum item.
    /// </returns>
    public static IObservable<TResult> Maximum<TObject, TKey, TResult>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TResult> valueSelector, TResult emptyValue = default)
        where TObject : notnull
        where TKey : notnull
        where TResult : struct, IComparable<TResult>
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);

        return source.ToChangesAndCollection().Calculate(valueSelector, MaxOrMin.Max, emptyValue);
    }

    /// <summary>
    /// Continually calculates the minimum value from the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The value to use when the underlying collection is empty.</param>
    /// <returns>A distinct observable of the minimums item.</returns>
    public static IObservable<TResult> Minimum<TObject, TResult>(this IObservable<IChangeSet<TObject>> source, Func<TObject, TResult> valueSelector, TResult emptyValue = default)
        where TObject : notnull
        where TResult : struct, IComparable<TResult>
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);

        return source.ToChangesAndCollection().Calculate(valueSelector, MaxOrMin.Min, emptyValue);
    }

    /// <summary>
    /// Continually calculates the minimum value from the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The value to use when the underlying collection is empty.</param>
    /// <returns>
    /// A distinct observable of the minimums item.
    /// </returns>
    public static IObservable<TResult> Minimum<TObject, TKey, TResult>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TResult> valueSelector, TResult emptyValue = default)
        where TObject : notnull
        where TKey : notnull
        where TResult : struct, IComparable<TResult>
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);

        return source.ToChangesAndCollection().Calculate(valueSelector, MaxOrMin.Min, emptyValue);
    }

    /// <summary>
    /// Executes the Calculate operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TResult">The type of the TResult value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="valueSelector">The valueSelector value.</param>
    /// <param name="maxOrMin">The maxOrMin value.</param>
    /// <param name="emptyValue">The emptyValue value.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<TResult> Calculate<TObject, TResult>(this IObservable<ChangesAndCollection<TObject>> source, Func<TObject, TResult> valueSelector, MaxOrMin maxOrMin, TResult emptyValue = default)
        where TResult : struct, IComparable<TResult>
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);

        return source.Scan(
            default(TResult?),
            (state, latest) =>
            {
                // Refresh and removal can refer to an object whose selected value has already
                // changed. Recompute from the current collection instead of comparing that value
                // with the cached extremum. This costs O(n) for those batches; pure adds stay incremental.
                if (latest.RequiresReset)
                {
                    return latest.Collection.Count == 0
                        ? default(TResult?)
                        : maxOrMin == MaxOrMin.Max ? latest.Collection.Max(valueSelector) : latest.Collection.Min(valueSelector);
                }

                var current = state;
                foreach (var change in latest.Changes)
                {
                    var value = valueSelector(change.Item);
                    if (!current.HasValue
                        || (maxOrMin == MaxOrMin.Max ? value.CompareTo(current.Value) > 0 : value.CompareTo(current.Value) < 0))
                    {
                        current = value;
                    }
                }

                return current;
            }).Select(t => t ?? emptyValue).DistinctUntilChanged();
    }

    /// <summary>
    /// Executes the ToChangesAndCollection operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TKey">The type of the TKey value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<ChangesAndCollection<TObject>> ToChangesAndCollection<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source.Publish(
            shared =>
            {
                var changes = shared.Select(static c => (
                    Changes: (IAggregateChangeSet<TObject>)new AggregateEnumerator<TObject, TKey>(c),
                    RequiresReset: c.Any(static change => change.Reason is ChangeReason.Update or ChangeReason.Remove or ChangeReason.Refresh)));
                var data = shared.ToCollection();
                return data.Zip(changes, (d, c) => new ChangesAndCollection<TObject>(c.Changes, d, c.RequiresReset));
            });
    }

    /// <summary>
    /// Executes the ToChangesAndCollection operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<ChangesAndCollection<TObject>> ToChangesAndCollection<TObject>(this IObservable<IChangeSet<TObject>> source)
        where TObject : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source.Publish(
            shared =>
            {
                var changes = shared.Select(static c => (
                    Changes: (IAggregateChangeSet<TObject>)new AggregateEnumerator<TObject>(c),
                    RequiresReset: c.Any(static change => change.Reason is ListChangeReason.Replace or ListChangeReason.Remove or ListChangeReason.RemoveRange or ListChangeReason.Clear or ListChangeReason.Refresh)));
                var data = shared.ToCollection();
                return data.Zip(changes, (d, c) => new ChangesAndCollection<TObject>(c.Changes, d, c.RequiresReset));
            });
    }

/// <summary>
/// Provides members for the ChangesAndCollection class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="changes">The changes value.</param>
/// <param name="collection">The collection value.</param>
/// <param name="requiresReset">Whether to recompute the extremum from the collection.</param>
private sealed class ChangesAndCollection<T>(IAggregateChangeSet<T> changes, IReadOnlyCollection<T> collection, bool requiresReset)
    {
        /// <summary>
        /// Gets the Changes value.
        /// </summary>
        public IAggregateChangeSet<T> Changes { get; } = changes;

        /// <summary>
        /// Gets the Collection value.
        /// </summary>
        public IReadOnlyCollection<T> Collection { get; } = collection;

        /// <summary>
        /// Gets a value indicating whether the projected extremum needs recomputation.
        /// </summary>
        public bool RequiresReset { get; } = requiresReset;
    }
}
