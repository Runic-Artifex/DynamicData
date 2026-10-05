// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Aggregation;
#else

namespace DynamicData.Aggregation;
#endif

/// <summary>
/// Average extensions.
/// </summary>
/// <remarks>
/// Empty sources emit the configured fallback after synchronous source initialization.
/// Direct list and cache overloads retain projected values and apply Refresh by index or key.
/// Aggregate streams have no keys, indexes or Refresh metadata: an empty aggregate batch
/// invalidates every retained occurrence. A Refresh mixed with membership changes cannot
/// be recovered after conversion through ForAggregation; use the direct overload for that case.
/// Aggregate removals also cannot identify an indexed occurrence of a repeated reference
/// with different stored projections; use the direct overload when occurrence indexes matter.
/// Nullable projections contribute zero and every item remains in the denominator.
/// </remarks>
public static class AvgEx
{
    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, int> valueSelector, int emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, int?> valueSelector, int emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, long> valueSelector, long emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, long?> valueSelector, long emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, double> valueSelector, double emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, double?> valueSelector, double emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<decimal> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, decimal> valueSelector, decimal emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<decimal> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, decimal?> valueSelector, decimal emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, float> valueSelector, float emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, float?> valueSelector, float emptyValue = 0)
        where TObject : notnull
        where TKey : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, int> valueSelector, int emptyValue = 0)
        where T : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, int?> valueSelector, int emptyValue = 0)
        where T : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, long> valueSelector, long emptyValue = 0)
        where T : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, long?> valueSelector, long emptyValue = 0)
        where T : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, double> valueSelector, double emptyValue = 0)
        where T : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, double?> valueSelector, double emptyValue = 0)
        where T : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<decimal> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, decimal> valueSelector, decimal emptyValue = 0)
        where T : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<decimal> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, decimal?> valueSelector, decimal emptyValue = 0)
        where T : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, float> valueSelector, float emptyValue = 0)
        where T : notnull => AvgProject(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<T>(this IObservable<IChangeSet<T>> source, Func<T, float?> valueSelector, float emptyValue = 0)
        where T : notnull => AvgProjectNullable(source, valueSelector).AvgProjected(emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, int> valueSelector, int emptyValue = 0)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return source.AvgCalc(valueSelector, emptyValue, (current, item) => new Avg<int>(current.Count + 1, current.Sum + item), (current, item) => new Avg<int>(current.Count - 1, current.Sum - item), values => values.Sum / (double)values.Count);
    }

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, int?> valueSelector, int emptyValue = 0) => source.Avg(AvgNullableSelector(valueSelector), emptyValue);

    /// <summary>
    /// Averages the specified value selector.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The empty value.</param>
    /// <returns>An observable of averages as a double value.</returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, long> valueSelector, long emptyValue = 0)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return source.AvgCalc(valueSelector, emptyValue, (current, item) => new Avg<long>(current.Count + 1, current.Sum + item), (current, item) => new Avg<long>(current.Count - 1, current.Sum - item), values => values.Sum / (double)values.Count);
    }

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, long?> valueSelector, long emptyValue = 0) => source.Avg(AvgNullableSelector(valueSelector), emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, double> valueSelector, double emptyValue = 0)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return source.AvgCalc(valueSelector, emptyValue, (current, item) => new Avg<double>(current.Count + 1, current.Sum + item), (current, item) => new Avg<double>(current.Count - 1, current.Sum - item), values => values.Sum / (double)values.Count);
    }

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<double> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, double?> valueSelector, double emptyValue = 0) => source.Avg(AvgNullableSelector(valueSelector), emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<decimal> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, decimal> valueSelector, decimal emptyValue = 0)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return source.AvgCalc(valueSelector, emptyValue, (current, item) => new Avg<decimal>(current.Count + 1, current.Sum + item), (current, item) => new Avg<decimal>(current.Count - 1, current.Sum - item), values => values.Sum / values.Count);
    }

    /// <summary>
    /// Averages the specified value selector.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="emptyValue">The empty value.</param>
    /// <returns>An observable of decimals with the averaged values.</returns>
    public static IObservable<decimal> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, decimal?> valueSelector, decimal emptyValue = 0) => source.Avg(AvgNullableSelector(valueSelector), emptyValue);

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, float> valueSelector, float emptyValue = 0)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return source.AvgCalc(valueSelector, emptyValue, (current, item) => new Avg<float>(current.Count + 1, current.Sum + item), (current, item) => new Avg<float>(current.Count - 1, current.Sum - item), values => values.Sum / values.Count);
    }

    /// <summary>
    /// Continuous calculation of the average of the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type to average.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="valueSelector">The function which returns the value.</param>
    /// <param name="emptyValue">The resulting average value when there is no data.</param>
    /// <returns>
    /// An observable of averages.
    /// </returns>
    public static IObservable<float> Avg<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, float?> valueSelector, float emptyValue = 0) => source.Avg(AvgNullableSelector(valueSelector), emptyValue);

    /// <summary>
    /// Executes the AvgCalc operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TValue">The type of the TValue value.</typeparam>
    /// <typeparam name="TResult">The type of the TResult value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="valueSelector">The valueSelector value.</param>
    /// <param name="fallbackValue">The fallbackValue value.</param>
    /// <param name="addAction">The addAction value.</param>
    /// <param name="removeAction">The removeAction value.</param>
    /// <param name="resultAction">The resultAction value.</param>
    /// <param name="retainProjections">Whether to retain aggregate occurrences for empty-batch invalidation.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<TResult> AvgCalc<TObject, TValue, TResult>(this IObservable<IAggregateChangeSet<TObject>> source, Func<TObject, TValue> valueSelector, TResult fallbackValue, Func<Avg<TValue>, TValue, Avg<TValue>> addAction, Func<Avg<TValue>, TValue, Avg<TValue>> removeAction, Func<Avg<TValue>, TResult> resultAction, bool retainProjections = true)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);
        ArgumentExceptionHelper.ThrowIfNull(addAction);
        ArgumentExceptionHelper.ThrowIfNull(removeAction);
        ArgumentExceptionHelper.ThrowIfNull(resultAction);

        return Observable.Defer(() =>
        {
            var projections = new List<(TObject Item, TValue Value)>();
            return source.Scan(default(Avg<TValue>), (state, changes) =>
            {
                var hasMembershipChanges = false;
                var mutations = retainProjections ? changes : AvgWithoutUnchangedPairs(changes, valueSelector);
                using var iterator = mutations.GetEnumerator();
                var hasCurrent = iterator.MoveNext();
                while (hasCurrent)
                {
                    var change = iterator.Current;
                    hasCurrent = iterator.MoveNext();
                    hasMembershipChanges = true;
                    if (retainProjections && change.Type == AggregateType.Remove && hasCurrent && iterator.Current.Type == AggregateType.Add)
                    {
                        var index = AvgOccurrenceIndex(projections, change.Item);
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
                        var index = AvgOccurrenceIndex(projections, change.Item);
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

                if (state.Count == 0)
                {
                    state = default;
                }

                return state;
            }).Select(values => values.Count == 0 ? fallbackValue : resultAction(values));
        }).AvgWithInitial(fallbackValue);
    }

    private static IObservable<IAggregateChangeSet<TValue>> AvgProject<TObject, TKey, TValue>(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TValue> valueSelector)
        where TObject : notnull
        where TKey : notnull
        where TValue : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);
        return source.Transform(valueSelector, transformOnRefresh: true).ForAggregation();
    }

    private static IObservable<IAggregateChangeSet<TValue>> AvgProject<TObject, TValue>(IObservable<IChangeSet<TObject>> source, Func<TObject, TValue> valueSelector)
        where TObject : notnull
        where TValue : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);
        return source.Transform(valueSelector, transformOnRefresh: true).ForAggregation();
    }

    private static IObservable<IAggregateChangeSet<TValue>> AvgProjectNullable<TObject, TKey, TValue>(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TValue?> valueSelector)
        where TObject : notnull
        where TKey : notnull
        where TValue : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return AvgProject(source, AvgNullableSelector(valueSelector));
    }

    private static IObservable<IAggregateChangeSet<TValue>> AvgProjectNullable<TObject, TValue>(IObservable<IChangeSet<TObject>> source, Func<TObject, TValue?> valueSelector)
        where TObject : notnull
        where TValue : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return AvgProject(source, AvgNullableSelector(valueSelector));
    }

    private static Func<TObject, TValue> AvgNullableSelector<TObject, TValue>(Func<TObject, TValue?> valueSelector)
        where TValue : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(valueSelector);
        return item => valueSelector(item).GetValueOrDefault();
    }

    private static IObservable<double> AvgProjected(this IObservable<IAggregateChangeSet<int>> source, int emptyValue) =>
        source.AvgCalc(static value => value, emptyValue,
            static (current, value) => new Avg<int>(current.Count + 1, current.Sum + value),
            static (current, value) => new Avg<int>(current.Count - 1, current.Sum - value),
            static values => values.Sum / (double)values.Count, retainProjections: false);

    private static IObservable<double> AvgProjected(this IObservable<IAggregateChangeSet<long>> source, long emptyValue) =>
        source.AvgCalc(static value => value, emptyValue,
            static (current, value) => new Avg<long>(current.Count + 1, current.Sum + value),
            static (current, value) => new Avg<long>(current.Count - 1, current.Sum - value),
            static values => values.Sum / (double)values.Count, retainProjections: false);

    private static IObservable<double> AvgProjected(this IObservable<IAggregateChangeSet<double>> source, double emptyValue) =>
        source.AvgCalc(static value => value, emptyValue,
            static (current, value) => new Avg<double>(current.Count + 1, current.Sum + value),
            static (current, value) => new Avg<double>(current.Count - 1, current.Sum - value),
            static values => values.Sum / (double)values.Count, retainProjections: false);

    private static IObservable<decimal> AvgProjected(this IObservable<IAggregateChangeSet<decimal>> source, decimal emptyValue) =>
        source.AvgCalc(static value => value, emptyValue,
            static (current, value) => new Avg<decimal>(current.Count + 1, current.Sum + value),
            static (current, value) => new Avg<decimal>(current.Count - 1, current.Sum - value),
            static values => values.Sum / values.Count, retainProjections: false);

    private static IObservable<float> AvgProjected(this IObservable<IAggregateChangeSet<float>> source, float emptyValue) =>
        source.AvgCalc(static value => value, emptyValue,
            static (current, value) => new Avg<float>(current.Count + 1, current.Sum + value),
            static (current, value) => new Avg<float>(current.Count - 1, current.Sum - value),
            static values => values.Sum / values.Count, retainProjections: false);

    private static int AvgOccurrenceIndex<TObject, TValue>(List<(TObject Item, TValue Value)> projections, TObject item)
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

    private static IEnumerable<AggregateItem<TObject>> AvgWithoutUnchangedPairs<TObject, TValue>(IEnumerable<AggregateItem<TObject>> changes, Func<TObject, TValue> valueSelector)
    {
        using var iterator = changes.GetEnumerator();
        var hasCurrent = iterator.MoveNext();
        while (hasCurrent)
        {
            var current = iterator.Current;
            hasCurrent = iterator.MoveNext();
            // Transform emits Remove/Add for a Replace or Update. Avoid perturbing
            // floating-point sums (including infinity) when the projection is unchanged.
            if (current.Type == AggregateType.Remove && hasCurrent && iterator.Current.Type == AggregateType.Add &&
                EqualityComparer<TValue>.Default.Equals(valueSelector(current.Item), valueSelector(iterator.Current.Item)))
            {
                hasCurrent = iterator.MoveNext();
                continue;
            }

            yield return current;
        }
    }

    private static IObservable<TResult> AvgWithInitial<TResult>(this IObservable<TResult> source, TResult fallbackValue) =>
        Observable.Create<TResult>(observer =>
        {
            var queue = new DeliveryQueue<TResult>(observer);
            var subscription = new SingleAssignmentDisposable();
            var initialized = false;
            var stopped = false;

            void Dispose()
            {
                queue.Dispose();
                using (queue.AcquireLock())
                {
                    stopped = true;
                }

                subscription.Dispose();
            }

            try
            {
                subscription.Disposable = source.Subscribe(
                    value =>
                    {
                        try
                        {
                            using var access = queue.AcquireLock();
                            if (!stopped)
                            {
                                initialized = true;
                                access.EnqueueNext(value);
                            }
                        }
                        catch
                        {
                            Dispose();
                            throw;
                        }
                    },
                    error =>
                    {
                        try
                        {
                            using var access = queue.AcquireLock();
                            if (!stopped)
                            {
                                stopped = true;
                                access.EnqueueError(error);
                            }
                        }
                        finally
                        {
                            subscription.Dispose();
                        }
                    },
                    () =>
                    {
                        try
                        {
                            using var access = queue.AcquireLock();
                            if (!stopped)
                            {
                                stopped = true;
                                if (!initialized)
                                {
                                    access.EnqueueNext(fallbackValue);
                                }

                                access.EnqueueCompleted();
                            }
                        }
                        catch
                        {
                            Dispose();
                            throw;
                        }
                        finally
                        {
                            subscription.Dispose();
                        }
                    });

                using var access = queue.AcquireLock();
                if (!stopped && !initialized)
                {
                    initialized = true;
                    access.EnqueueNext(fallbackValue);
                }
            }
            catch (Exception error)
            {
                if (queue.HasDeliveryFailure)
                {
                    Dispose();
                    throw;
                }

                try
                {
                    using var access = queue.AcquireLock();
                    if (!stopped)
                    {
                        stopped = true;
                        access.EnqueueError(error);
                    }
                }
                finally
                {
                    subscription.Dispose();
                }
            }

            return Disposable.Create(Dispose);
        });
}
