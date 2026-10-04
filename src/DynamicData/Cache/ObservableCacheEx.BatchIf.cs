// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

using DynamicData.Reactive.Cache.Internal;
#else

using DynamicData.Cache.Internal;
#endif

// ReSharper disable once CheckNamespace
#if REACTIVE_SHIM

namespace DynamicData.Reactive;
#else

namespace DynamicData;
#endif

/// <summary>
/// Extensions for dynamic data.
/// </summary>
public static partial class ObservableCacheEx
{
    /// <summary>
    /// Provides an overload of <c>Run</c> for the supplied arguments.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TKey">The type of the TKey value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="pauseIfTrueSelector">The pauseIfTrueSelector value.</param>
    /// <param name="initialPauseState">The initialPauseState value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <returns>The resulting observable sequence.</returns>
    /// <remarks>This overload delegates to the primary overload with default <c>initialPauseState: false</c>.</remarks>
    public static IObservable<IChangeSet<TObject, TKey>> BatchIf<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, IObservable<bool> pauseIfTrueSelector, bool initialPauseState, IScheduler? scheduler = null)
        where TObject : notnull
        where TKey : notnull => new BatchIf<TObject, TKey>(source, pauseIfTrueSelector, null, initialPauseState, scheduler: scheduler).Run();

    /// <summary>
    /// Provides an overload of <c>BatchIf</c> for the supplied arguments.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TKey">The type of the TKey value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="pauseIfTrueSelector">The pauseIfTrueSelector value.</param>
    /// <param name="timeOut">The timeOut value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <returns>The resulting observable sequence.</returns>
    /// <remarks>This overload omits <c>initialPauseState</c> (defaults to <see langword="false"/>) but accepts a timeout.</remarks>
    public static IObservable<IChangeSet<TObject, TKey>> BatchIf<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, IObservable<bool> pauseIfTrueSelector, TimeSpan? timeOut, IScheduler? scheduler = null)
        where TObject : notnull
        where TKey : notnull => BatchIf(source, pauseIfTrueSelector, false, timeOut, scheduler);

    /// <summary>
    /// Conditionally buffers changesets while a pause signal is active, then flushes all buffered
    /// changes as a single merged changeset when the signal resumes.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;</c> to conditionally buffer.</param>
    /// <param name="pauseIfTrueSelector">An <c>IObservable&lt;bool&gt;</c> that when <see langword="true"/>, buffering begins. When <see langword="false"/>, the buffer is flushed.</param>
    /// <param name="initialPauseState">If <see langword="true"/>, starts in a paused (buffering) state.</param>
    /// <param name="timeOut">A <see cref="TimeSpan"/> that maximum time the buffer stays open. When elapsed, the buffer is flushed regardless of pause state.</param>
    /// <param name="scheduler">The <see cref="IScheduler"/> for timeout timing.</param>
    /// <returns>An observable that emits changesets, buffered or passthrough depending on pause state.</returns>
    /// <remarks>
    /// <para>
    /// While paused, incoming changesets are accumulated. On resume (or timeout), all buffered changesets
    /// are merged into a single changeset and emitted. While not paused, changesets pass through immediately.
    /// </para>
    /// <list type="table">
    /// <listheader><term>Event</term><description>Behavior</description></listheader>
    /// <item><term>Add</term><description>Buffered while paused; forwarded immediately while active.</description></item>
    /// <item><term>Update</term><description>Buffered while paused; forwarded immediately while active.</description></item>
    /// <item><term>Remove</term><description>Buffered while paused; forwarded immediately while active.</description></item>
    /// <item><term>Refresh</term><description>Buffered while paused; forwarded immediately while active.</description></item>
    /// <item><term>OnError</term><description>Buffered data is lost.</description></item>
    /// <item><term>OnCompleted</term><description>Any remaining buffered data is flushed before completion.</description></item>
    /// </list>
    /// <para><b>Worth noting:</b> If the source completes while paused, buffered data IS flushed before OnCompleted. However, if the source errors while paused, buffered data is lost.</para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="pauseIfTrueSelector"/> is <see langword="null"/>.</exception>
    /// <seealso><c>Batch&lt;TObject, TKey&gt;</c></seealso>
    /// <seealso><c>BufferInitial&lt;TObject, TKey&gt;</c></seealso>
    public static IObservable<IChangeSet<TObject, TKey>> BatchIf<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, IObservable<bool> pauseIfTrueSelector, bool initialPauseState, TimeSpan? timeOut, IScheduler? scheduler = null)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(pauseIfTrueSelector);

        return new BatchIf<TObject, TKey>(source, pauseIfTrueSelector, timeOut, initialPauseState, scheduler: scheduler).Run();
    }

    /// <inheritdoc cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, bool, TimeSpan?, IScheduler?)"/>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;</c> to conditionally buffer.</param>
    /// <param name="pauseIfTrueSelector">An <c>IObservable&lt;bool&gt;</c> that controls buffering: <see langword="true"/> begins buffering, <see langword="false"/> flushes the buffer.</param>
    /// <param name="scheduler">
    /// An optional <see cref="IScheduler"/>, retained only for source compatibility. It does not schedule any work on this
    /// overload: the operator schedules work solely to honour a <see cref="TimeSpan"/> timeout, and this overload always
    /// forwards a null timeout, so the value is unused whether or not <paramref name="timer"/> is supplied. A supplied
    /// <paramref name="timer"/> controls its own scheduling. Use <see cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, bool, TimeSpan?, IScheduler?)"/> for scheduler-driven timing.
    /// </param>
    /// <param name="timer">An optional <see cref="IObservable{Unit}"/> timer. The buffer is flushed each time the timer produces a value, and buffering ceases when it completes.</param>
    /// <remarks>
    /// <para>This overload starts unpaused. It exists so that a named <c>timer</c> argument, with or without a named <c>scheduler</c>, is a supported call shape without supplying an initial pause state.</para>
    /// <para>
    /// The parameter order and the absence of an <c>initialPauseState</c> parameter are both deliberate, and changing either
    /// reintroduces a compile error in previously valid code. Declaring <paramref name="scheduler"/> before <paramref name="timer"/>
    /// keeps the fourth positional parameter incompatible with <see cref="IScheduler"/>, so <c>BatchIf(pauseIfTrueSelector, null, scheduler)</c>
    /// still binds unambiguously to the <see cref="TimeSpan"/> timeout overload. Omitting <c>initialPauseState</c> keeps a fully named
    /// call that supplies an initial pause state bound unambiguously to the overload that accepts one.
    /// </para>
    /// <para>Supply an initial pause state through <see cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, bool, IObservable{Unit}, IScheduler)"/>.</para>
    /// </remarks>
    /// <seealso cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, bool, IObservable{Unit}, IScheduler)"/>
    public static IObservable<IChangeSet<TObject, TKey>> BatchIf<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, IObservable<bool> pauseIfTrueSelector, IScheduler? scheduler = null, IObservable<Unit>? timer = null)
        where TObject : notnull
        where TKey : notnull => BatchIf(source, pauseIfTrueSelector, false, timer, scheduler);

    /// <inheritdoc cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, bool, TimeSpan?, IScheduler?)"/>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;</c> to conditionally buffer.</param>
    /// <param name="pauseIfTrueSelector">An <c>IObservable&lt;bool&gt;</c> that controls buffering: <see langword="true"/> begins buffering, <see langword="false"/> flushes the buffer.</param>
    /// <param name="initialPauseState">If <see langword="true"/>, starts in a paused (buffering) state.</param>
    /// <param name="timer">An optional <c>IObservable&lt;Unit&gt;</c> timer. The buffer is flushed each time the timer produces a value, and buffering ceases when it completes.</param>
    /// <param name="scheduler">An optional <see cref="IScheduler"/> for scheduling work.</param>
    /// <returns>The resulting observable sequence.</returns>
    /// <remarks>This overload accepts an explicit timer observable instead of a <see cref="TimeSpan"/> timeout.</remarks>
    /// <seealso cref="BatchIf{TObject, TKey}(IObservable{IChangeSet{TObject, TKey}}, IObservable{bool}, IScheduler, IObservable{Unit})"/>
    public static IObservable<IChangeSet<TObject, TKey>> BatchIf<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, IObservable<bool> pauseIfTrueSelector, bool initialPauseState, IObservable<Unit>? timer, IScheduler? scheduler = null)
        where TObject : notnull
        where TKey : notnull => new BatchIf<TObject, TKey>(source, pauseIfTrueSelector, null, initialPauseState, timer, scheduler).Run();
}
