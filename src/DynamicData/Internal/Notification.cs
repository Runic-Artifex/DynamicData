// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Internal;
#else

namespace DynamicData.Internal;
#endif

/// <summary>
/// A lightweight notification struct for delivery queues. Discriminates
/// OnNext, OnError, and OnCompleted with an explicit kind independent of
/// the payload, preserving null references, nullable values and value-type defaults.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
internal readonly struct Notification<T>
{
    /// <summary>
    /// The _value field.
    /// </summary>
    private readonly T? _value;

    /// <summary>The notification kind, independent of its payload.</summary>
    private readonly NotificationKind _kind;

    /// <summary>
    /// The _error field.
    /// </summary>
    private readonly Exception? _error;

    /// <summary>
    /// Initializes a new instance of the <see cref="Notification{T}"/> struct.
    /// </summary>
    /// <param name="value">The value value.</param>
    /// <param name="error">The error value.</param>
    /// <param name="kind">The notification kind.</param>
    private Notification(T? value, Exception? error, NotificationKind kind)
    {
        _value = value;
        _error = error;
        _kind = kind;
    }

    /// <summary>Creates an OnNext notification.</summary>
    /// <param name="value">The value value.</param>
    /// <returns>The result of the operation.</returns>
    public static Notification<T> CreateNext(T value) => new(value, null, NotificationKind.Next);

    /// <summary>Creates an OnError notification (terminal).</summary>
    /// <param name="error">The error value.</param>
    /// <returns>The result of the operation.</returns>
    public static Notification<T> CreateError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        return new(default, error, NotificationKind.Error);
    }

    /// <summary>Creates an OnCompleted notification (terminal).</summary>
    /// <returns>The result of the operation.</returns>
    public static Notification<T> CreateCompleted() => new(default, null, NotificationKind.Completed);

    /// <summary>Gets whether this is an OnError notification.</summary>
    public bool IsError => _kind == NotificationKind.Error;

    /// <summary>Gets whether this is a terminal notification (OnError or OnCompleted).</summary>
    public bool IsTerminal => _kind != NotificationKind.Next;

    /// <summary>Delivers this notification to the specified observer.</summary>
    /// <param name="observer">The observer value.</param>
    public void Accept(IObserver<T> observer)
    {
        if (_kind == NotificationKind.Next)
        {
            observer.OnNext(_value!);
        }
        else if (_kind == NotificationKind.Error)
        {
            observer.OnError(_error!);
        }
        else
        {
            observer.OnCompleted();
        }
    }

    private enum NotificationKind : byte
    {
        Completed,
        Next,
        Error,
    }
}
