// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

namespace NSerf.Serf.Events;

/// <summary>
/// Base interface for all Serf events.
/// Maps to: Go's Event interface in event.go
/// </summary>
public interface IEvent
{
    /// <summary>
    /// Gets the type of this event.
    /// </summary>
    EventType EventType();

    /// <summary>
    /// Gets a string representation of this event.
    /// </summary>
    string ToString();
}
