// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Threading.Channels;
using NSerf.Client.Responses;

namespace NSerf.Client;

/// <summary>
/// A stream opened on an <see cref="RpcClient"/> connection (the 'stream' or 'monitor' command).
/// Records pushed by the agent are read from <see cref="Records"/>; <see cref="StopAsync"/> sends the
/// 'stop' command for <see cref="Seq"/> and completes the channel.
/// Maps to: Go's StreamHandle in client/rpc_client.go
/// </summary>
/// <typeparam name="TRecord">Record type: <see cref="StreamEvent"/> for 'stream', <see cref="LogEntry"/> for 'monitor'.</typeparam>
public sealed class RpcStreamHandle<TRecord> : IAsyncDisposable
{
    private readonly RpcClient _client;

    internal RpcStreamHandle(RpcClient client, ulong seq, ChannelReader<TRecord> records)
    {
        _client = client;
        Seq = seq;
        Records = records;
    }

    /// <summary>
    /// Seq of the command that opened the stream; this is the handle passed to <see cref="RpcClient.StopAsync"/>.
    /// </summary>
    public ulong Seq { get; }

    /// <summary>
    /// Records pushed by the agent. The channel completes once the stream is stopped or the connection is lost.
    /// </summary>
    public ChannelReader<TRecord> Records { get; }

    /// <summary>
    /// Completes when no more records will arrive (stopped, or connection lost).
    /// </summary>
    public Task Completion => Records.Completion;

    /// <summary>
    /// Enumerates the records until the stream ends.
    /// </summary>
    public IAsyncEnumerable<TRecord> ReadAllAsync(CancellationToken cancellationToken = default)
        => Records.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Sends the 'stop' command for this stream and completes <see cref="Records"/>.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
        => _client.StopAsync(Seq, cancellationToken);

    /// <summary>
    /// Best-effort stop: swallows failures caused by a closed connection or a disposed client.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _client.StopQuietlyAsync(Seq);
    }
}

/// <summary>
/// A query started with <see cref="RpcClient.StartQueryAsync"/>. Ack, response and done records are read
/// from <see cref="Records"/>; the channel completes once the 'done' record arrives (or the query is stopped).
/// Maps to: Go's queryHandler + QueryParam.AckCh/RespCh in client/rpc_client.go
/// </summary>
public sealed class RpcQueryHandle : IAsyncDisposable
{
    private readonly RpcClient _client;

    internal RpcQueryHandle(RpcClient client, ulong seq, ulong id, ChannelReader<QueryRecord> records)
    {
        _client = client;
        Seq = seq;
        Id = id;
        Records = records;
    }

    /// <summary>
    /// Query ID assigned by the agent (from the initial response).
    /// </summary>
    public ulong Id { get; }

    /// <summary>
    /// Seq of the query command; records are streamed under it and it is the handle for 'stop'.
    /// </summary>
    public ulong Seq { get; }

    /// <summary>
    /// Ack / response / done records in arrival order. Completes after the 'done' record.
    /// </summary>
    public ChannelReader<QueryRecord> Records { get; }

    /// <summary>
    /// Completes once the query is done (deadline reached), stopped, or the connection is lost.
    /// </summary>
    public Task Completion => Records.Completion;

    /// <summary>
    /// Enumerates the records until the query is done.
    /// </summary>
    public IAsyncEnumerable<QueryRecord> ReadAllAsync(CancellationToken cancellationToken = default)
        => Records.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Stops receiving records for this query (sends 'stop' for <see cref="Seq"/>).
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
        => _client.StopAsync(Seq, cancellationToken);

    /// <summary>
    /// Best-effort stop unless the query already finished.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Completion.IsCompleted)
        {
            return;
        }

        await _client.StopQuietlyAsync(Seq);
    }
}
