// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

// Runic (DD-044): deliberately in the root DynamicData namespace so that every
// source.CombineLatest(..., selector) extension call in DynamicData.* code binds here instead of
// ReactiveUI.Primitives' CombineLatest (see ScanExtensions). The System.Reactive flavour keeps
// Observable.CombineLatest.
namespace DynamicData;

/// <summary>
/// Primitives-flavour replacement for <c>CombineLatest</c> with a result selector that follows the
/// System.Reactive contract.
/// </summary>
/// <remarks>
/// ReactiveUI.Primitives 8.4 and 9.0 <c>CombineLatest</c> throws result-selector exceptions back to
/// the producer instead of delivering them as <c>OnError</c>, so failures in callbacks such as the
/// <c>WhenChanged</c> result selector or the <c>TrueForAll</c>/<c>TrueForAny</c> condition were thrown
/// to the code raising the change. These overloads combine with Primitives' <c>CombineLatest</c>
/// (same subscription, serialization and completion behaviour) and apply the selector in a
/// <c>Select</c>, which delivers its failures as <c>OnError</c>. The selector still runs inside the
/// combined emission, so it stays serialized.
/// </remarks>
internal static class CombineLatestExtensions
{
    /// <summary>Combines the latest values of two sources. An exception thrown by the selector is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T1">The type of the first source's values.</typeparam>
    /// <typeparam name="T2">The type of the second source's values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source1">The first source.</param>
    /// <param name="source2">The second source.</param>
    /// <param name="selector">Projects the latest values into a result.</param>
    /// <returns>The projected latest values.</returns>
    public static IObservable<TResult> CombineLatest<T1, T2, TResult>(this IObservable<T1> source1, IObservable<T2> source2, Func<T1, T2, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return PrimitivesLinqExtensions.CombineLatest(source1, source2, static (v1, v2) => (v1, v2))
            .Select(values => selector(values.v1, values.v2));
    }

    /// <summary>Combines the latest values of three sources. An exception thrown by the selector is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T1">The type of the first source's values.</typeparam>
    /// <typeparam name="T2">The type of the second source's values.</typeparam>
    /// <typeparam name="T3">The type of the third source's values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source1">The first source.</param>
    /// <param name="source2">The second source.</param>
    /// <param name="source3">The third source.</param>
    /// <param name="selector">Projects the latest values into a result.</param>
    /// <returns>The projected latest values.</returns>
    public static IObservable<TResult> CombineLatest<T1, T2, T3, TResult>(this IObservable<T1> source1, IObservable<T2> source2, IObservable<T3> source3, Func<T1, T2, T3, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return PrimitivesLinqExtensions.CombineLatest(source1, source2, source3, static (v1, v2, v3) => (v1, v2, v3))
            .Select(values => selector(values.v1, values.v2, values.v3));
    }

    /// <summary>Combines the latest values of four sources. An exception thrown by the selector is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T1">The type of the first source's values.</typeparam>
    /// <typeparam name="T2">The type of the second source's values.</typeparam>
    /// <typeparam name="T3">The type of the third source's values.</typeparam>
    /// <typeparam name="T4">The type of the fourth source's values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source1">The first source.</param>
    /// <param name="source2">The second source.</param>
    /// <param name="source3">The third source.</param>
    /// <param name="source4">The fourth source.</param>
    /// <param name="selector">Projects the latest values into a result.</param>
    /// <returns>The projected latest values.</returns>
    public static IObservable<TResult> CombineLatest<T1, T2, T3, T4, TResult>(this IObservable<T1> source1, IObservable<T2> source2, IObservable<T3> source3, IObservable<T4> source4, Func<T1, T2, T3, T4, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return PrimitivesLinqExtensions.CombineLatest(source1, source2, source3, source4, static (v1, v2, v3, v4) => (v1, v2, v3, v4))
            .Select(values => selector(values.v1, values.v2, values.v3, values.v4));
    }

    /// <summary>Combines the latest values of five sources. An exception thrown by the selector is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T1">The type of the first source's values.</typeparam>
    /// <typeparam name="T2">The type of the second source's values.</typeparam>
    /// <typeparam name="T3">The type of the third source's values.</typeparam>
    /// <typeparam name="T4">The type of the fourth source's values.</typeparam>
    /// <typeparam name="T5">The type of the fifth source's values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source1">The first source.</param>
    /// <param name="source2">The second source.</param>
    /// <param name="source3">The third source.</param>
    /// <param name="source4">The fourth source.</param>
    /// <param name="source5">The fifth source.</param>
    /// <param name="selector">Projects the latest values into a result.</param>
    /// <returns>The projected latest values.</returns>
    public static IObservable<TResult> CombineLatest<T1, T2, T3, T4, T5, TResult>(this IObservable<T1> source1, IObservable<T2> source2, IObservable<T3> source3, IObservable<T4> source4, IObservable<T5> source5, Func<T1, T2, T3, T4, T5, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return PrimitivesLinqExtensions.CombineLatest(source1, source2, source3, source4, source5, static (v1, v2, v3, v4, v5) => (v1, v2, v3, v4, v5))
            .Select(values => selector(values.v1, values.v2, values.v3, values.v4, values.v5));
    }

    /// <summary>Combines the latest values of six sources. An exception thrown by the selector is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T1">The type of the first source's values.</typeparam>
    /// <typeparam name="T2">The type of the second source's values.</typeparam>
    /// <typeparam name="T3">The type of the third source's values.</typeparam>
    /// <typeparam name="T4">The type of the fourth source's values.</typeparam>
    /// <typeparam name="T5">The type of the fifth source's values.</typeparam>
    /// <typeparam name="T6">The type of the sixth source's values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source1">The first source.</param>
    /// <param name="source2">The second source.</param>
    /// <param name="source3">The third source.</param>
    /// <param name="source4">The fourth source.</param>
    /// <param name="source5">The fifth source.</param>
    /// <param name="source6">The sixth source.</param>
    /// <param name="selector">Projects the latest values into a result.</param>
    /// <returns>The projected latest values.</returns>
    public static IObservable<TResult> CombineLatest<T1, T2, T3, T4, T5, T6, TResult>(this IObservable<T1> source1, IObservable<T2> source2, IObservable<T3> source3, IObservable<T4> source4, IObservable<T5> source5, IObservable<T6> source6, Func<T1, T2, T3, T4, T5, T6, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return PrimitivesLinqExtensions.CombineLatest(source1, source2, source3, source4, source5, source6, static (v1, v2, v3, v4, v5, v6) => (v1, v2, v3, v4, v5, v6))
            .Select(values => selector(values.v1, values.v2, values.v3, values.v4, values.v5, values.v6));
    }
}
#endif
