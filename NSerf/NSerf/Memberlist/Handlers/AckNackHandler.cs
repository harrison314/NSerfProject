// Ported from: github.com/hashicorp/memberlist
// Copyright (c) Boolhak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace NSerf.Memberlist.Handlers;

/// <summary>
/// Handles acknowledgment and negative acknowledgment responses.
/// </summary>
public class AckNackHandler(ILogger? logger = null)
{
    private readonly ConcurrentDictionary<uint, AckHandler> _handlers = new();

    /// <summary>
    /// Sets an ack handler for a sequence number. When the timeout elapses without an ack,
    /// the handler is removed and <paramref name="nackFn"/> is invoked.
    /// </summary>
    public void SetAckHandler(uint seqNo, Action<byte[], DateTimeOffset> ackFn, Action? nackFn, TimeSpan timeout)
    {
        SetAckHandler(seqNo, ackFn, nackFn, nackFn, timeout);
    }

    /// <summary>
    /// Sets an ack handler for a sequence number with distinct nack and timeout callbacks
    /// (Go's setProbeChannels): <paramref name="nackFn"/> runs for every nack received while the
    /// handler is registered, <paramref name="timeoutFn"/> runs once when the timeout elapses
    /// without an ack, after the handler has been removed.
    /// </summary>
    public void SetAckHandler(uint seqNo, Action<byte[], DateTimeOffset> ackFn, Action? nackFn, Action? timeoutFn, TimeSpan timeout)
    {
        var handler = new AckHandler
        {
            AckFn = ackFn,
            NackFn = nackFn,
            Timer = new Timer(_ =>
            {
                if (!_handlers.TryRemove(seqNo, out var h)) return;
                h.Dispose();
                timeoutFn?.Invoke();
            }, null, timeout, Timeout.InfiniteTimeSpan)
        };

        _handlers[seqNo] = handler;
    }

    /// <summary>
    /// Invokes ack handler for a sequence number.
    /// </summary>
    public void InvokeAck(uint seqNo, byte[] payload, DateTimeOffset timestamp)
    {
        if (!_handlers.TryRemove(seqNo, out var handler)) return;
        handler.Timer?.Dispose();
        handler.AckFn?.Invoke(payload, timestamp);
        handler.Dispose();
    }

    /// <summary>
    /// Invokes the nack handler for a sequence number. Like Go's invokeNackHandler this leaves the
    /// handler registered: several intermediaries may nack while an ack (or the timeout) is still pending.
    /// </summary>
    public void InvokeNack(uint seqNo)
    {
        if (!_handlers.TryGetValue(seqNo, out var handler)) return;
        handler.NackFn?.Invoke();
    }

    /// <summary>
    /// Clears all handlers.
    /// </summary>
    public void Clear()
    {
        foreach (var handler in _handlers.Values)
        {
            handler.Dispose();
        }
        _handlers.Clear();
        logger?.LogDebug("Cleared all handlers");
    }

    /// <summary>
    /// Gets the count of pending handlers.
    /// </summary>
    public int PendingCount => _handlers.Count;
}
