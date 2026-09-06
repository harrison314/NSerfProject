// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using MessagePack;
using NSerf.Client;
using NSerf.Client.Responses;

namespace NSerf.Agent.RPC;

/// <summary>
/// QueryResponseStream handles streaming query acks and responses back to the RPC client.
/// Every record is framed as ResponseHeader{Seq = query command seq} + <see cref="QueryRecord"/>.
/// Disposing the stream (the stop command, or the session closing) stops further records.
/// Maps to: Go's queryResponseStream in ipc_query_response_stream.go
/// </summary>
internal sealed class QueryResponseStream(
    SemaphoreSlim writeLock,
    Stream stream,
    ulong seq,
    Serf.QueryResponse queryResponse) : IDisposable
{
    private static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.None);

    private readonly CancellationTokenSource _stopCts = new();
    private volatile bool _disposed;

    /// <summary>
    /// Seq of the query command that opened this stream.
    /// </summary>
    public ulong Seq => seq;

    /// <summary>
    /// Stream is a long-running routine that streams query results back to the client.
    /// Maps to: Go's Stream() method in ipc_query_response_stream.go
    /// </summary>
    public async Task StreamAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopCts.Token);
            var sessionToken = sessionCts.Token;

            // Setup timer for query deadline
            var remaining = queryResponse.Deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                await SendDoneAsync(sessionToken);
                return;
            }

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
            timeoutCts.CancelAfter(remaining);

            var ackCh = queryResponse.AckCh;
            var respCh = queryResponse.ResponseCh;

            try
            {
                while (!timeoutCts.Token.IsCancellationRequested)
                {
                    // Try to read from an ack channel
                    if (ackCh != null && ackCh.TryRead(out var ack))
                    {
                        await SendAckAsync(ack, timeoutCts.Token);
                        continue;
                    }

                    // Try to read from a response channel
                    if (respCh.TryRead(out var resp))
                    {
                        await SendResponseAsync(resp.From, resp.Payload, timeoutCts.Token);
                        continue;
                    }

                    // Serf closes both channels once the query has finished
                    if (respCh.Completion.IsCompleted && (ackCh == null || ackCh.Completion.IsCompleted))
                    {
                        break;
                    }

                    // Small delay to avoid busy-waiting
                    await Task.Delay(10, timeoutCts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // Timeout reached or cancellation requested
            }

            // Send done marker (Go: sends the done record once the deadline passes); the timeout token
            // is already cancelled at this point so the write uses the session token.
            await SendDoneAsync(sessionToken);
        }
        catch (Exception)
        {
            // Swallow exceptions - client may have disconnected or the stream was stopped
        }
        finally
        {
            _stopCts.Dispose();
        }
    }

    private async Task SendAckAsync(string from, CancellationToken cancellationToken)
    {
        var record = new QueryRecord
        {
            Type = QueryRecordType.Ack,
            From = from,
            Payload = []
        };

        await SendRecordAsync(record, cancellationToken);
    }

    private async Task SendResponseAsync(string from, byte[] payload, CancellationToken cancellationToken)
    {
        var record = new QueryRecord
        {
            Type = QueryRecordType.Response,
            From = from,
            Payload = payload
        };

        await SendRecordAsync(record, cancellationToken);
    }

    private async Task SendDoneAsync(CancellationToken cancellationToken)
    {
        var record = new QueryRecord
        {
            Type = QueryRecordType.Done,
            From = string.Empty,
            Payload = []
        };

        await SendRecordAsync(record, cancellationToken);
    }

    private async Task SendRecordAsync(QueryRecord record, CancellationToken cancellationToken)
    {
        await writeLock.WaitAsync(cancellationToken);
        try
        {
            // Re-check under the lock: a stop that completed while we waited must not be followed by a record
            if (_disposed)
                return;

            var header = new ResponseHeader { Seq = seq, Error = string.Empty };
            var headerBytes = MessagePackSerializer.Serialize(header, MsgPackOptions, cancellationToken);
            await stream.WriteAsync(headerBytes, cancellationToken);

            var recordBytes = MessagePackSerializer.Serialize(record, MsgPackOptions, cancellationToken);
            await stream.WriteAsync(recordBytes, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }
        finally
        {
            writeLock.Release();
        }
    }

    /// <summary>
    /// Stops the stream: no further records (not even 'done') are written for this seq.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        try
        {
            _stopCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already torn down
        }
    }
}
