// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net.Sockets;
using MessagePack;
using NSerf.Client;
using NSerf.Serf.Events;

namespace NSerf.Agent.RPC;

/// <summary>
/// RPC event handler for the stream command.
/// Every event is written as a <see cref="ResponseHeader"/> carrying the seq of the stream command,
/// followed by the <see cref="Client.Responses.StreamEvent"/> record (Go: eventStream.sendEvent in ipc_event_stream.go).
/// The filter uses Go's syntax: "*" (or empty) streams everything, otherwise a comma-separated
/// list of "type" or "type:name" entries such as "member-join,user:deploy".
/// </summary>
public sealed class RpcEventHandler : IEventHandler, IDisposable
{
    private readonly NetworkStream _stream;
    private readonly SemaphoreSlim _writeLock;
    private readonly CancellationToken _cancellationToken;
    private readonly Func<Query, ulong>? _registerQuery;
    private readonly EventFilter[] _filters;
    private volatile bool _disposed;

    private static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.None);

    public RpcEventHandler(NetworkStream stream, SemaphoreSlim writeLock, string? eventFilter, CancellationToken cancellationToken)
        : this(stream, writeLock, 0, eventFilter, null, cancellationToken)
    {
    }

    /// <param name="stream">Connection the records are written to.</param>
    /// <param name="writeLock">Session write lock that serialises every write on the connection.</param>
    /// <param name="seq">Seq of the stream command; every record is framed with a header carrying it.</param>
    /// <param name="eventFilter">Go-style event filter ("*", "member-join,user:deploy", ...).</param>
    /// <param name="registerQuery">
    /// Registers a streamed query as pending on the session and returns the session-scoped query ID
    /// sent as <see cref="Client.Responses.StreamEvent.QueryID"/> (Go: IPCClient.RegisterQuery).
    /// </param>
    /// <param name="cancellationToken">Cancelled when the session ends.</param>
    public RpcEventHandler(
        NetworkStream stream,
        SemaphoreSlim writeLock,
        ulong seq,
        string? eventFilter,
        Func<Query, ulong>? registerQuery,
        CancellationToken cancellationToken)
    {
        _stream = stream;
        _writeLock = writeLock;
        Seq = seq;
        _registerQuery = registerQuery;
        _cancellationToken = cancellationToken;
        _filters = ParseFilters(eventFilter);
    }

    /// <summary>
    /// Seq of the stream command that opened this stream.
    /// </summary>
    public ulong Seq { get; }

    public void HandleEvent(IEvent @event)
    {
        if (_disposed || _cancellationToken.IsCancellationRequested)
            return;

        // Filter by event type/name if specified (no filters = stream everything)
        if (_filters.Length > 0 && !_filters.Any(f => f.Matches(@event)))
            return;

        try
        {
            var streamEvent = ConvertToStreamEvent(@event);
            var header = new ResponseHeader { Seq = Seq, Error = string.Empty };
            var headerBytes = MessagePackSerializer.Serialize(header, MsgPackOptions, _cancellationToken);
            var eventBytes = MessagePackSerializer.Serialize(streamEvent, MsgPackOptions, _cancellationToken);

            _writeLock.Wait(_cancellationToken);
            try
            {
                // Re-check under the lock: a stop that completed while we waited must not be followed by a record
                if (_disposed)
                    return;

                _stream.Write(headerBytes);
                _stream.Write(eventBytes);
                _stream.Flush();
            }
            finally
            {
                _writeLock.Release();
            }
        }
        catch
        {
            // Stream closed or error
        }
    }

    private Client.Responses.StreamEvent ConvertToStreamEvent(IEvent @event)
    {
        var eventType = @event.EventType();
        var streamEvent = new Client.Responses.StreamEvent
        {
            Event = eventType.String().ToLowerInvariant()
        };

        // Handle different event types
        switch (@event)
        {
            case MemberEvent memberEvent:
                streamEvent.Members = [.. memberEvent.Members.Select(m => new Client.Responses.Member
                {
                    Name = m.Name,
                    Addr = m.Addr.GetAddressBytes(), // same encoding as the members command
                    Port = m.Port,
                    Tags = m.Tags,
                    Status = m.Status.ToString().ToLowerInvariant(),
                    ProtocolMin = m.ProtocolMin,
                    ProtocolMax = m.ProtocolMax,
                    ProtocolCur = m.ProtocolCur,
                    DelegateMin = m.DelegateMin,
                    DelegateMax = m.DelegateMax,
                    DelegateCur = m.DelegateCur
                })];
                break;

            case UserEvent userEvent:
                streamEvent.LTime = userEvent.LTime;
                streamEvent.Name = userEvent.Name;
                streamEvent.Payload = userEvent.Payload;
                break;

            case Query query:
                streamEvent.LTime = query.LTime;
                streamEvent.Name = query.Name;
                streamEvent.Payload = query.Payload;
                streamEvent.From = query.SourceNode();
                if (_registerQuery != null)
                {
                    streamEvent.QueryID = _registerQuery(query);
                }
                break;
        }

        return streamEvent;
    }

    /// <summary>
    /// Parses a stream filter. Throws <see cref="ArgumentException"/> for unknown event types.
    /// </summary>
    internal static EventFilter[] ParseFilters(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter) || filter.Trim() == "*")
            return [];

        return filter
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(EventFilter.Parse)
            .ToArray();
    }

    public void Dispose()
    {
        // Resources are external; disposing only stops further records from being written.
        _disposed = true;
    }
}
