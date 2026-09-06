// Ported from: github.com/hashicorp/memberlist
// Copyright (c) Boolhak, Inc.
// SPDX-License-Identifier: MPL-2.0

namespace NSerf.Memberlist.Broadcast;

/// <summary>
/// High-level broadcast queue wrapper.
/// </summary>
public class BroadcastQueue(TransmitLimitedQueue queue)
{
    /// <summary>
    /// Queues a simple byte array broadcast.
    /// </summary>
    public void QueueBytes(byte[] data)
    {
        queue.QueueBroadcast(new SimpleBroadcast(data));
    }

    /// <summary>
    /// Queues a broadcast and waits until it has been transmitted the configured number of
    /// times (or was invalidated by a newer broadcast), bounded by <paramref name="timeout"/>.
    /// Returns true if the broadcast finished before the timeout, false otherwise.
    /// Mirrors Go's pattern of queueing a broadcast with a notify channel and selecting on it.
    /// </summary>
    public async Task<bool> QueueBytesAsync(byte[] data, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var notifier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        queue.QueueBroadcast(new NotifyingBroadcast(data, notifier));

        try
        {
            await notifier.Task.WaitAsync(timeout, cancellationToken);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// Queues a named broadcast that can be invalidated.
    /// </summary>
    public void QueueNamed(string name, byte[] data)
    {
        queue.QueueBroadcast(new NamedBroadcast(name, data));
    }

    /// <summary>
    /// Gets broadcasts up to the specified limits.
    /// </summary>
    public List<byte[]> GetBroadcasts(int overhead, int limit)
    {
        return queue.GetBroadcasts(overhead, limit);
    }

    /// <summary>
    /// Gets the number of queued broadcasts.
    /// </summary>
    public int Count => queue.NumQueued();

    /// <summary>
    /// Resets the queue.
    /// </summary>
    public void Reset() => queue.Reset();

    /// <summary>
    /// Prunes old broadcasts.
    /// </summary>
    public void Prune(int maxRetain) => queue.Prune(maxRetain);
}

/// <summary>
/// Simple broadcast implementation.
/// </summary>
internal class SimpleBroadcast(byte[] data) : IBroadcast
{
    public bool Invalidates(IBroadcast other) => false;
    public byte[] Message() => data;
    public void Finished() { }
}

/// <summary>
/// Named broadcast implementation.
/// </summary>
internal class NamedBroadcast(string name, byte[] data) : INamedBroadcast
{
    public string Name() => name;
    public bool Invalidates(IBroadcast other) => false;
    public byte[] Message() => data;
    public void Finished() { }
}

/// <summary>
/// Broadcast that notifies when transmission completes.
/// Matches Go's broadcast notification pattern.
/// </summary>
internal class NotifyingBroadcast(byte[] data, TaskCompletionSource notifier) : IBroadcast
{
    public bool Invalidates(IBroadcast other) => false;
    public byte[] Message() => data;

    public void Finished()
    {
        // Signal that broadcast has been sent (matches Go closing the notification channel)
        notifier.TrySetResult();
    }
}
