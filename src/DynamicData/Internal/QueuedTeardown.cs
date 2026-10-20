// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Internal;
#else

namespace DynamicData.Internal;
#endif

/// <summary>
/// Runs an operator's subscription teardown (disposing or releasing the items it tracks) so that it never
/// overlaps a delivery of the queue created by <c>SynchronizeSafe(QueuedTeardown)</c>, without waiting for that
/// delivery. If a delivery is in progress on another thread, the teardown runs on that thread as soon as the
/// delivery returns; otherwise, including when called from the delivering thread, it runs before
/// <see cref="Run"/> returns. Only the first call runs the teardown.
/// </summary>
internal sealed class QueuedTeardown
{
    private readonly Lock _gate = new();
    private Action<Action>? _runWhenNotDelivering;
    private bool _isRequested;

    /// <summary>Runs <paramref name="teardown"/> once no delivery is in progress on another thread.</summary>
    /// <param name="teardown">The teardown. Must be safe to run on the delivering thread.</param>
    public void Run(Action teardown)
    {
        Action<Action>? runWhenNotDelivering;
        lock (_gate)
        {
            if (_isRequested)
            {
                return;
            }

            _isRequested = true;
            runWhenNotDelivering = _runWhenNotDelivering;
        }

        // Without a queue nothing can be delivering.
        if (runWhenNotDelivering is null)
        {
            teardown();
        }
        else
        {
            runWhenNotDelivering(teardown);
        }
    }

    /// <summary>Attaches the queue whose deliveries the teardown must not overlap.</summary>
    /// <param name="runWhenNotDelivering">The queue's <c>RunWhenNotDelivering</c>.</param>
    internal void Attach(Action<Action> runWhenNotDelivering)
    {
        lock (_gate)
        {
            _runWhenNotDelivering = runWhenNotDelivering;
        }
    }
}
