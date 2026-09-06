// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net.Sockets;
using MessagePack;
using NSerf.Client;
using static NSerf.Agent.CircularLogWriter;

namespace NSerf.Agent.RPC;

/// <summary>
/// RPC log handler for monitor command streaming.
/// Every line is written as a <see cref="ResponseHeader"/> carrying the seq of the monitor command,
/// followed by a <see cref="Client.Responses.LogEntry"/> record (Go: logStream.Stream in ipc_log_stream.go).
/// </summary>
public sealed class RpcLogHandler : ILogHandler, IDisposable
{
    private readonly NetworkStream _stream;
    private readonly SemaphoreSlim _writeLock;
    private readonly CancellationToken _cancellationToken;
    private volatile bool _disposed;

    private static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.None);

    public RpcLogHandler(NetworkStream stream, SemaphoreSlim writeLock, CancellationToken cancellationToken)
        : this(stream, writeLock, 0, cancellationToken)
    {
    }

    /// <param name="stream">Connection the records are written to.</param>
    /// <param name="writeLock">Session write lock that serialises every write on the connection.</param>
    /// <param name="seq">Seq of the monitor command; every record is framed with a header carrying it.</param>
    /// <param name="cancellationToken">Cancelled when the session ends.</param>
    public RpcLogHandler(NetworkStream stream, SemaphoreSlim writeLock, ulong seq, CancellationToken cancellationToken)
    {
        _stream = stream;
        _writeLock = writeLock;
        Seq = seq;
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Seq of the monitor command that opened this stream.
    /// </summary>
    public ulong Seq { get; }

    public void HandleLog(string log)
    {
        if (_disposed || _cancellationToken.IsCancellationRequested)
            return;

        try
        {
            var header = new ResponseHeader { Seq = Seq, Error = string.Empty };
            var headerBytes = MessagePackSerializer.Serialize(header, MsgPackOptions, _cancellationToken);

            // The client deserializes each line as a LogEntry record (Go: logRecord), not a bare string
            var entry = new NSerf.Client.Responses.LogEntry { Log = log };
            var logBytes = MessagePackSerializer.Serialize(entry, MsgPackOptions, _cancellationToken);

            _writeLock.Wait(_cancellationToken);
            try
            {
                // Re-check under the lock: a stop that completed while we waited must not be followed by a record
                if (_disposed)
                    return;

                _stream.Write(headerBytes);
                _stream.Write(logBytes);
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

    public void Dispose()
    {
        // Resources are external; disposing only stops further records from being written.
        _disposed = true;
    }
}
