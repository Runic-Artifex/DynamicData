// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Aggregation;
#else

namespace DynamicData.Aggregation;
#endif

/// <summary>
/// Extensions for calculating sample standard deviation.
/// </summary>
/// <remarks>
/// Computes sample standard deviation using incremental central moments. Integral and decimal
/// selectors use decimal accumulators; their central moments must fit in <see cref="decimal"/>.
/// Floating-point selectors retain the precision limits of double arithmetic.
/// Direct list and cache overloads retain projected values and apply Refresh by index or key:
/// a refreshed item whose projection changed replaces its previous value, and removals and
/// updates subtract the value that was added rather than re-evaluating a mutated item.
/// Aggregate streams have no keys, indexes or Refresh metadata: aggregate removals subtract the
/// retained projection of the matching occurrence (reference, then equality), and an empty aggregate
/// batch re-evaluates every retained occurrence. A Refresh mixed with membership changes cannot
/// be recovered after conversion through ForAggregation; use the direct overload for that case.
/// </remarks>
public static class StdDevEx
{
    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IChangeSet<T>> source, Func<T, int> valueSelector, int fallbackValue)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (decimal)valueSelector(item)).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false).Select(static value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IChangeSet<T>> source, Func<T, long> valueSelector, long fallbackValue)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (decimal)valueSelector(item)).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false).Select(static value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IChangeSet<T>> source, Func<T, double> valueSelector, double fallbackValue)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, valueSelector).StdDevCalc(static value => value, (double)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<decimal> StdDev<T>(this IObservable<IChangeSet<T>> source, Func<T, decimal> valueSelector, decimal fallbackValue)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, valueSelector).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IChangeSet<T>> source, Func<T, float> valueSelector, float fallbackValue = 0)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (double)valueSelector(item)).StdDevCalc(static value => value, (double)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, int> valueSelector, int fallbackValue)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (decimal)valueSelector(item)).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false).Select(static value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, long> valueSelector, long fallbackValue)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (decimal)valueSelector(item)).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false).Select(static value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, double> valueSelector, double fallbackValue)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, valueSelector).StdDevCalc(static value => value, (double)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<decimal> StdDev<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, decimal> valueSelector, decimal fallbackValue)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, valueSelector).StdDevCalc(static value => value, (decimal)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, float> valueSelector, float fallbackValue = 0)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return StdDevProject(source, item => (double)valueSelector(item)).StdDevCalc(static value => value, (double)fallbackValue, retainProjections: false);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, int> valueSelector, int fallbackValue = 0)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        return source.StdDevCalc(t => (decimal)valueSelector(t), (decimal)fallbackValue).Select(value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, long> valueSelector, long fallbackValue = 0)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        return source.StdDevCalc(t => (decimal)valueSelector(t), (decimal)fallbackValue).Select(value => (double)value);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<decimal> StdDev<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, decimal> valueSelector, decimal fallbackValue = 0M)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        return source.StdDevCalc(valueSelector, fallbackValue);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, double> valueSelector, double fallbackValue = 0)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        return source.StdDevCalc(valueSelector, fallbackValue);
    }

    /// <summary>
    /// Continual computation of the standard deviation of the  values in the underlying data source.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <param name="fallbackValue">The fallback value.</param>
    /// <returns>An observable which emits the standard deviation value.</returns>
    public static IObservable<double> StdDev<T>(this IObservable<IAggregateChangeSet<T>> source, Func<T, float> valueSelector, float fallbackValue = 0)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);
        return source.StdDevCalc(t => valueSelector(t), fallbackValue);
    }

    /// <summary>
    /// Executes the StdDevCalc operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="valueSelector">The valueSelector value.</param>
    /// <param name="fallbackValue">The fallbackValue value.</param>
    /// <param name="retainProjections">Whether to retain aggregate occurrences; false when items are already projected values.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<double> StdDevCalc<TObject>(this IObservable<IAggregateChangeSet<TObject>> source, Func<TObject, double> valueSelector, double fallbackValue, bool retainProjections = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return source.ScanProjected<TObject, double, StdDev<double>>(valueSelector, Add, Remove, static values => values.Count == 0, retainProjections)
            .Select(values => values.Count < 2 ? fallbackValue : Math.Sqrt(Math.Max(values.SumOfSquaredDifferences, 0D) / (values.Count - 1)));
    }

    /// <summary>
    /// Executes the StdDevCalc operation.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="valueSelector">The valueSelector value.</param>
    /// <param name="fallbackValue">The fallbackValue value.</param>
    /// <param name="retainProjections">Whether to retain aggregate occurrences; false when items are already projected values.</param>
    /// <returns>The result of the operation.</returns>
    private static IObservable<decimal> StdDevCalc<TObject>(this IObservable<IAggregateChangeSet<TObject>> source, Func<TObject, decimal> valueSelector, decimal fallbackValue, bool retainProjections = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return source.ScanProjected<TObject, decimal, StdDev<decimal>>(valueSelector, Add, Remove, static values => values.Count == 0, retainProjections)
            .Select(values => values.Count < 2 ? fallbackValue : Sqrt(Math.Max(values.SumOfSquaredDifferences, 0M) / (values.Count - 1)));
    }

    /// <summary>
    /// Projects a keyed change set to immutable values so that Refresh becomes remove-old/add-new.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the projected value.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <returns>The projected aggregate stream.</returns>
    private static IObservable<IAggregateChangeSet<TValue>> StdDevProject<TObject, TKey, TValue>(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TValue> valueSelector)
        where TObject : notnull
        where TKey : notnull
        where TValue : notnull => source.Transform(valueSelector, transformOnRefresh: true).ForAggregation();

    /// <summary>
    /// Projects a list change set to immutable values so that Refresh becomes remove-old/add-new.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TValue">The type of the projected value.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="valueSelector">The value selector.</param>
    /// <returns>The projected aggregate stream.</returns>
    private static IObservable<IAggregateChangeSet<TValue>> StdDevProject<TObject, TValue>(IObservable<IChangeSet<TObject>> source, Func<TObject, TValue> valueSelector)
        where TObject : notnull
        where TValue : notnull => source.Transform(valueSelector, transformOnRefresh: true).ForAggregation();

    private static StdDev<double> Add(StdDev<double> current, double item)
    {
        var count = current.Count + 1;
        var delta = item - current.Mean;
        var mean = current.Mean + (delta / count);
        var delta2 = item - mean;
        return new StdDev<double>(count, mean, current.SumOfSquaredDifferences + (delta * delta2));
    }

    private static StdDev<double> Remove(StdDev<double> current, double item)
    {
        if (current.Count <= 1)
        {
            return default;
        }

        var count = current.Count - 1;
        var delta = item - current.Mean;
        var mean = current.Mean - (delta / count);
        if (count == 1)
        {
            return new StdDev<double>(count, mean, 0D);
        }

        var delta2 = item - mean;
        return new StdDev<double>(count, mean, current.SumOfSquaredDifferences - (delta * delta2));
    }

    private static StdDev<decimal> Add(StdDev<decimal> current, decimal item)
    {
        var count = current.Count + 1;
        var delta = item - current.Mean;
        var mean = current.Mean + (delta / count);
        var delta2 = item - mean;
        return new StdDev<decimal>(count, mean, current.SumOfSquaredDifferences + (delta * delta2));
    }

    private static StdDev<decimal> Remove(StdDev<decimal> current, decimal item)
    {
        if (current.Count <= 1)
        {
            return default;
        }

        var count = current.Count - 1;
        var delta = item - current.Mean;
        var mean = current.Mean - (delta / count);
        if (count == 1)
        {
            return new StdDev<decimal>(count, mean, 0M);
        }

        var delta2 = item - mean;
        return new StdDev<decimal>(count, mean, current.SumOfSquaredDifferences - (delta * delta2));
    }

    /// <summary>
    /// Executes the Sqrt operation.
    /// </summary>
    /// <param name="x">The x value.</param>
    /// <returns>The result of the operation.</returns>
    private static decimal Sqrt(decimal x)
    {
        if (x < 0)
        {
            throw new OverflowException("Cannot calculate square root from a negative number");
        }

        var current = (decimal)Math.Sqrt((double)x);
        var previous = 0M;
        while (current != 0M)
        {
            var next = (current + (x / current)) / 2;
            // Decimal rounding can alternate between two adjacent values.
            if (next == current || next == previous)
            {
                return next;
            }

            previous = current;
            current = next;
        }

        return current;
    }
}
