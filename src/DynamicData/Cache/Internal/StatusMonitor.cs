// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Cache.Internal;
#else

namespace DynamicData.Cache.Internal;
#endif

/// <summary>
/// Provides members for the StatusMonitor class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="source">The source value.</param>
internal sealed class StatusMonitor<T>(IObservable<T> source)
{
    private readonly IObservable<T> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<ConnectionStatus> Run() =>
        _source.Select(static _ => ConnectionStatus.Loaded)
            .Concat(Observable.Return(ConnectionStatus.Completed))
            .Catch<ConnectionStatus, Exception>(static error => Observable.Return(ConnectionStatus.Errored).Concat(Observable.Throw<ConnectionStatus>(error)))
            .StartWith(ConnectionStatus.Pending)
            .DistinctUntilChanged();
}
