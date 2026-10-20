// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM

namespace DynamicData.Reactive.Internal;
#else
namespace DynamicData.Internal;
#endif

/// <summary>
/// Runic (DD-044, fork issue #39): the subscriptions <c>SubscribeMany</c>'s factory has created and not yet
/// disposed, so that disposing <c>SubscribeMany</c> disposes each of them, including one that has not reached
/// <c>DisposeMany</c>.
/// </summary>
/// <remarks>
/// The factory runs inside <c>Transform</c>, before <c>DisposeMany</c> tracks its result. A subscription the
/// factory makes can deliver to the subscriber at once (a <c>MergeMany</c> child that emits while it is
/// subscribed), and a subscriber exception then disposes <c>SubscribeMany</c> while the factory is still running.
/// <c>DisposeMany</c> disposed only what it had tracked, and the changeset carrying the new subscriptions was
/// dropped once its subscription had been disposed (System.Reactive's <c>Observable.Create</c> detaches its
/// observer), so they stayed attached to their sources. Here a subscription is owned from the moment the factory
/// returns it; one returned after disposal is disposed immediately.
/// </remarks>
internal sealed class OwnedSubscriptions : IDisposable
{
    private readonly Lock _gate = new();
    private HashSet<Owned>? _live = [];

    /// <summary>
    /// Takes ownership of a subscription the factory created.
    /// </summary>
    /// <param name="subscription">The subscription.</param>
    /// <returns>A disposable that disposes the subscription and releases its ownership.</returns>
    public IDisposable Own(IDisposable subscription)
    {
        if (subscription is null)
        {
            // Passed on unchanged, as before.
            return subscription!;
        }

        var owned = new Owned(this, subscription);
        lock (_gate)
        {
            if (_live is not null)
            {
                _live.Add(owned);
                return owned;
            }
        }

        // Disposed while the factory was running.
        owned.Dispose();
        return owned;
    }

    /// <summary>
    /// Disposes every owned subscription that has not been disposed yet, and any owned afterwards.
    /// </summary>
    public void Dispose()
    {
        HashSet<Owned>? live;
        lock (_gate)
        {
            live = _live;
            _live = null;
        }

        if (live is null)
        {
            return;
        }

        foreach (var owned in live)
        {
            owned.Dispose();
        }
    }

    private void Release(Owned owned)
    {
        lock (_gate)
        {
            _live?.Remove(owned);
        }
    }

    private sealed class Owned(OwnedSubscriptions owner, IDisposable subscription) : IDisposable
    {
        [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in Dispose, once, through Interlocked.Exchange.")]
        private IDisposable? _subscription = subscription;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _subscription, null) is { } disposable)
            {
                owner.Release(this);
                disposable.Dispose();
            }
        }
    }
}
