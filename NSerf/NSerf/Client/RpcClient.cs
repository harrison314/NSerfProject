// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Buffers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using MessagePack;

namespace NSerf.Client;

/// <summary>
/// RPC client for connecting to Serf agent.
/// A background reader demultiplexes every response by seq: ordinary commands complete a one-shot handler,
/// while stream / monitor / query records are pushed into per-stream channels until the stream is stopped.
/// Maps to: Go's client/rpc_client.go
/// </summary>
public class RpcClient(RpcConfig config) : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Records buffered per stream before the oldest unread ones are dropped (Go drops when its channel is full).
    /// </summary>
    private const int StreamBufferSize = 1024;

    private readonly RpcConfig _config = config ?? throw new ArgumentNullException(nameof(config));
    private TcpClient? _tcpClient;
    private NetworkStream? _stream;
    private MessagePackStreamReader? _reader;
    private ulong _seqCounter;
    private bool _disposed;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // Pending handlers keyed by request seq (Go: RPCClient.dispatch)
    private readonly Dictionary<ulong, ISeqHandler> _handlers = [];
    private readonly object _handlersLock = new();
    private bool _readerStopped;
    private Exception? _connectionError;
    private CancellationTokenSource? _readerCts;
    private Task? _readerTask;

    // MessagePack options matching Go's codec
    private static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.None);

    /// <summary>
    /// Connects to the Serf agent RPC endpoint.
    /// Maps to: Go's ClientFromConfig() in rpc_client.go
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_tcpClient != null)
            throw new InvalidOperationException("Already connected");

        try
        {
            _tcpClient = new TcpClient();

            // Parse address (format: "host:port")
            var parts = _config.Address.Split(':');
            if (parts.Length != 2)
                throw new ArgumentException($"Invalid address format: {_config.Address}. Expected 'host:port'");

            var host = parts[0];
            if (!int.TryParse(parts[1], out var port))
                throw new ArgumentException($"Invalid port: {parts[1]}");

            // Connect with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_config.Timeout);

            await _tcpClient.ConnectAsync(host, port, cts.Token);
            _tcpClient.NoDelay = true;
            _stream = _tcpClient.GetStream();
            _reader = new MessagePackStreamReader(_stream, leaveOpen: true);

            // Start the response reader (Go: go c.listen())
            var readerCts = new CancellationTokenSource();
            lock (_handlersLock)
            {
                _readerStopped = false;
                _connectionError = null;
            }

            _readerCts = readerCts;
            _readerTask = Task.Run(() => ReadLoopAsync(_reader, readerCts.Token), CancellationToken.None);

            // Perform handshake
            await HandshakeAsync(cts.Token);

            // Perform authentication if auth key provided
            if (!string.IsNullOrEmpty(_config.AuthKey))
            {
                await AuthenticateAsync(_config.AuthKey, cts.Token);
            }
        }
        catch
        {
            // Cleanup on failure
            await TearDownConnectionAsync();
            throw;
        }
    }

    /// <summary>
    /// Performs the initial handshake with the agent.
    /// Maps to: Go's handshake() in rpc_client.go
    /// </summary>
    private async Task HandshakeAsync(CancellationToken cancellationToken)
    {
        var request = new HandshakeRequest
        {
            Version = RpcConstants.MaxIpcVersion
        };

        var response = await CallAsync<HandshakeRequest, object>(RpcCommands.Handshake, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Handshake failed: {response.Error}");
    }

    /// <summary>
    /// Performs authentication with the agent.
    /// Maps to: Go's auth() in rpc_client.go
    /// </summary>
    private async Task AuthenticateAsync(string authKey, CancellationToken cancellationToken)
    {
        var request = new AuthRequest
        {
            AuthKey = authKey
        };

        var response = await CallAsync<AuthRequest, object>(RpcCommands.Auth, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Authentication failed: {response.Error}");
    }

    /// <summary>
    /// Sends a request to the agent.
    /// Maps to: Go's send() in rpc_client.go
    /// </summary>
    private async Task SendRequestAsync<TRequest>(
        RequestHeader header,
        TRequest? request,
        CancellationToken cancellationToken)
    {
        if (_stream == null)
            throw new InvalidOperationException("Not connected");

        // Encode header and body up front; they go out as one unit
        var buffer = new ArrayBufferWriter<byte>();
        MessagePackSerializer.Serialize(buffer, header, MsgPackOptions, cancellationToken);
        if (!Equals(request, default(TRequest)))
        {
            MessagePackSerializer.Serialize(buffer, request, MsgPackOptions, cancellationToken);
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            // Set a write deadline (timeout)
            _tcpClient!.SendTimeout = (int)_config.Timeout.TotalMilliseconds;

            // The caller's token no longer interrupts the write itself: a request must reach the
            // agent whole or not at all, otherwise the connection could not be resynchronised.
            // The write is bounded by the configured timeout instead.
            using var writeCts = new CancellationTokenSource(_config.Timeout);
            await _stream.WriteAsync(buffer.WrittenMemory, writeCts.Token);
            await _stream.FlushAsync(writeCts.Token);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Sends a request and waits for its (single) response.
    /// Maps to: Go's genericRPC() in rpc_client.go
    /// </summary>
    private async Task<ResponseResult<TResponse>> CallAsync<TRequest, TResponse>(
        string command,
        TRequest? request,
        bool expectBody,
        CancellationToken cancellationToken)
    {
        var seq = GetNextSeq();
        var handler = new OneShotHandler<TResponse>(expectBody);
        RegisterHandler(seq, handler);

        var sent = false;
        try
        {
            await SendRequestAsync(new RequestHeader { Command = command, Seq = seq }, request, cancellationToken);
            sent = true;
            return await handler.Response.WaitAsync(cancellationToken);
        }
        catch
        {
            // Once the request is on the wire its response still arrives: the handler stays
            // registered so the reader consumes the header (and body) for this seq and drops it.
            // Only a request that never left the client is deregistered here.
            if (!sent)
            {
                DeregisterHandler(seq, handler);
            }

            throw;
        }
    }

    /// <summary>
    /// Sends a stream-opening command (stream / monitor / query) and waits for its acknowledgement.
    /// The handler stays registered so the records that follow are routed to it.
    /// </summary>
    private async Task<(ulong Seq, StreamHandler<TInit, TRecord> Handler, ResponseResult<TInit> Init)> OpenStreamAsync<TRequest, TInit, TRecord>(
        string command,
        TRequest request,
        bool initHasBody,
        Func<TRecord, bool>? isTerminal,
        CancellationToken cancellationToken)
    {
        var seq = GetNextSeq();
        var handler = new StreamHandler<TInit, TRecord>(initHasBody, isTerminal);
        RegisterHandler(seq, handler);

        var sent = false;
        try
        {
            await SendRequestAsync(new RequestHeader { Command = command, Seq = seq }, request, cancellationToken);
            sent = true;
            var init = await handler.Init.WaitAsync(cancellationToken);
            if (!string.IsNullOrEmpty(init.Error))
            {
                DeregisterHandler(seq, handler);
            }

            return (seq, handler, init);
        }
        catch
        {
            if (!sent)
            {
                DeregisterHandler(seq, handler);
            }
            else
            {
                // The agent has opened (or is about to open) the stream: keep the handler so its
                // acknowledgement and records are consumed, and ask the agent to stop it.
                _ = StopQuietlyAsync(seq);
            }

            throw;
        }
    }

    /// <summary>
    /// Background reader: reads every response header and dispatches it (and its body, when one follows)
    /// to the handler registered for its seq. Maps to: Go's listen() in rpc_client.go
    /// </summary>
    private async Task ReadLoopAsync(MessagePackStreamReader reader, CancellationToken cancellationToken)
    {
        Exception? error = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var headerBytes = await reader.ReadAsync(cancellationToken);
                if (!headerBytes.HasValue)
                {
                    error = new RpcException("Connection closed");
                    break;
                }

                ResponseHeader header;
                try
                {
                    header = MessagePackSerializer.Deserialize<ResponseHeader>(headerBytes.Value, MsgPackOptions, cancellationToken);
                }
                catch (MessagePackSerializationException)
                {
                    // A record whose stream was already stopped locally (body without a consumer); resync on the next message
                    continue;
                }

                ISeqHandler? handler;
                lock (_handlersLock)
                {
                    _handlers.TryGetValue(header.Seq, out handler);
                }

                if (handler == null)
                {
                    // No pending handler for this seq (Go logs and drops it)
                    continue;
                }

                ReadOnlySequence<byte>? body = null;
                if (handler.ExpectsBody(header))
                {
                    var bodyBytes = await reader.ReadAsync(cancellationToken);
                    if (!bodyBytes.HasValue)
                    {
                        error = new RpcException("Connection closed while reading response body");
                        break;
                    }

                    body = bodyBytes.Value;
                }

                var keep = handler.Handle(header, body);
                if (!keep)
                {
                    DeregisterHandler(header.Seq, handler);
                }
            }

            error ??= new RpcException("Client closed");
        }
        catch (OperationCanceledException)
        {
            error = new RpcException("Client closed");
        }
        catch (Exception ex)
        {
            error = new RpcException($"Connection lost: {ex.Message}", ex);
        }

        FailPendingHandlers(error);
    }

    private void RegisterHandler(ulong seq, ISeqHandler handler)
    {
        lock (_handlersLock)
        {
            if (_readerStopped)
            {
                throw _connectionError ?? new RpcException("Connection closed");
            }

            if (_reader == null)
            {
                throw new InvalidOperationException("Not connected");
            }

            _handlers[seq] = handler;
        }
    }

    private void DeregisterHandler(ulong seq, ISeqHandler? expected = null)
    {
        lock (_handlersLock)
        {
            if (expected == null || (_handlers.TryGetValue(seq, out var current) && ReferenceEquals(current, expected)))
            {
                _handlers.Remove(seq);
            }
        }
    }

    /// <summary>
    /// Completes every pending handler with <paramref name="error"/> and ends every stream.
    /// </summary>
    private void FailPendingHandlers(Exception error)
    {
        ISeqHandler[] pending;
        lock (_handlersLock)
        {
            _readerStopped = true;
            _connectionError ??= error;
            pending = [.. _handlers.Values];
            _handlers.Clear();
        }

        foreach (var handler in pending)
        {
            handler.Cleanup(error);
        }
    }

    private ulong GetNextSeq()
    {
        return Interlocked.Increment(ref _seqCounter);
    }

    public bool IsConnected => _tcpClient?.Connected ?? false;

    // ========== Membership Commands ==========

    public async Task<Responses.Member[]> MembersAsync(CancellationToken cancellationToken = default)
    {
        var response = await CallAsync<object, Responses.MembersResponse>(RpcCommands.Members, null, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Members command failed: {response.Error}");

        return response.Body?.Members ?? [];
    }

    public async Task<Responses.Member[]> MembersFilteredAsync(
        Dictionary<string, string>? tags = null,
        string? status = null,
        string? name = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Requests.MembersFilteredRequest
        {
            Tags = tags ?? [],
            Status = status ?? string.Empty,
            Name = name ?? string.Empty
        };

        var response = await CallAsync<Requests.MembersFilteredRequest, Responses.MembersResponse>(RpcCommands.MembersFiltered, request, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"MembersFiltered command failed: {response.Error}");

        return response.Body?.Members ?? [];
    }

    public async Task<int> JoinAsync(string[] addresses, bool replay = false, CancellationToken cancellationToken = default)
    {
        var request = new Requests.JoinRequest { Existing = addresses, Replay = replay };

        var response = await CallAsync<Requests.JoinRequest, Responses.JoinResponse>(RpcCommands.Join, request, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Join command failed: {response.Error}");

        return response.Body?.Num ?? 0;
    }

    public async Task ForceLeaveAsync(string node, bool prune = false, CancellationToken cancellationToken = default)
    {
        var request = new Requests.ForceLeaveRequest { Node = node, Prune = prune };

        var response = await CallAsync<Requests.ForceLeaveRequest, object>(RpcCommands.ForceLeave, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"ForceLeave command failed: {response.Error}");
    }

    public async Task LeaveAsync(CancellationToken cancellationToken = default)
    {
        var response = await CallAsync<object, object>(RpcCommands.Leave, null, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Leave command failed: {response.Error}");
    }

    // ========== Event Commands ==========

    public async Task UserEventAsync(string name, byte[]? payload = null, bool coalesce = false, CancellationToken cancellationToken = default)
    {
        var request = new Requests.EventRequest
        {
            Name = name,
            Payload = payload ?? [],
            Coalesce = coalesce
        };

        var response = await CallAsync<Requests.EventRequest, object>(RpcCommands.Event, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"UserEvent command failed: {response.Error}");
    }

    // ========== Key Management Commands ==========

    public async Task<Responses.KeyResponse> InstallKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        var request = new Requests.KeyRequest { Key = key };

        var response = await CallAsync<Requests.KeyRequest, Responses.KeyResponse>(RpcCommands.InstallKey, request, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"InstallKey command failed: {response.Error}");

        return response.Body ?? new Responses.KeyResponse();
    }

    public async Task<Responses.KeyResponse> UseKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        var request = new Requests.KeyRequest { Key = key };

        var response = await CallAsync<Requests.KeyRequest, Responses.KeyResponse>(RpcCommands.UseKey, request, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"UseKey command failed: {response.Error}");

        return response.Body ?? new Responses.KeyResponse();
    }

    public async Task<Responses.KeyResponse> RemoveKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        var request = new Requests.KeyRequest { Key = key };

        var response = await CallAsync<Requests.KeyRequest, Responses.KeyResponse>(RpcCommands.RemoveKey, request, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"RemoveKey command failed: {response.Error}");

        return response.Body ?? new Responses.KeyResponse();
    }

    public async Task<Responses.KeyResponse> ListKeysAsync(CancellationToken cancellationToken = default)
    {
        var response = await CallAsync<object, Responses.KeyResponse>(RpcCommands.ListKeys, null, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"ListKeys command failed: {response.Error}");

        return response.Body ?? new Responses.KeyResponse();
    }

    // ========== Query Commands ==========

    /// <summary>
    /// Starts a query and returns a handle exposing its ack / response / done records.
    /// Maps to: Go's Query() in rpc_client.go
    /// </summary>
    public async Task<RpcQueryHandle> StartQueryAsync(
        string name,
        byte[]? payload = null,
        string[]? filterNodes = null,
        Dictionary<string, string>? filterTags = null,
        bool requestAck = false,
        uint timeoutSeconds = 0,
        CancellationToken cancellationToken = default)
    {
        var request = new Requests.QueryRequest
        {
            Name = name,
            Payload = payload ?? [],
            FilterNodes = filterNodes ?? [],
            FilterTags = filterTags ?? [],
            RequestAck = requestAck,
            Timeout = timeoutSeconds
        };

        var (seq, handler, init) = await OpenStreamAsync<Requests.QueryRequest, Responses.QueryResponse, Responses.QueryRecord>(
            RpcCommands.Query,
            request,
            initHasBody: true,
            isTerminal: record => record.Type == Responses.QueryRecordType.Done,
            cancellationToken);

        if (!string.IsNullOrEmpty(init.Error))
            throw new RpcException($"Query command failed: {init.Error}");

        return new RpcQueryHandle(this, seq, init.Body?.Id ?? 0, handler.Records);
    }

    /// <summary>
    /// Starts a query and returns its ID. The query's records are routed to an internal handler
    /// (and dropped) until the 'done' record so they never pollute the connection.
    /// </summary>
    public async Task<ulong> QueryAsync(
        string name,
        byte[]? payload = null,
        string[]? filterNodes = null,
        Dictionary<string, string>? filterTags = null,
        bool requestAck = false,
        uint timeoutSeconds = 0,
        CancellationToken cancellationToken = default)
    {
        var handle = await StartQueryAsync(name, payload, filterNodes, filterTags, requestAck, timeoutSeconds, cancellationToken);
        return handle.Id;
    }

    /// <summary>
    /// Answers a query received on a 'stream' (identified by <see cref="Responses.StreamEvent.QueryID"/>).
    /// Maps to: Go's Respond() in rpc_client.go
    /// </summary>
    public async Task RespondAsync(ulong queryId, byte[] payload, CancellationToken cancellationToken = default)
    {
        var request = new Requests.RespondRequest { ID = queryId, Payload = payload };

        var response = await CallAsync<Requests.RespondRequest, object>(RpcCommands.Respond, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Respond command failed: {response.Error}");
    }

    // ========== Other Commands ==========

    public async Task UpdateTagsAsync(Dictionary<string, string>? tags = null, string[]? deleteTags = null, CancellationToken cancellationToken = default)
    {
        var request = new Requests.TagsRequest
        {
            Tags = tags ?? [],
            DeleteTags = deleteTags ?? []
        };

        var response = await CallAsync<Requests.TagsRequest, object>(RpcCommands.Tags, request, expectBody: false, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"UpdateTags command failed: {response.Error}");
    }

    public async Task<Dictionary<string, Dictionary<string, string>>> StatsAsync(CancellationToken cancellationToken = default)
    {
        var response = await CallAsync<object, Responses.StatsResponse>(RpcCommands.Stats, null, expectBody: true, cancellationToken);

        if (!string.IsNullOrEmpty(response.Error))
            throw new RpcException($"Stats command failed: {response.Error}");

        return response.Body?.Stats ?? [];
    }

    public async Task<Responses.Coordinate?> GetCoordinateAsync(string node, CancellationToken cancellationToken = default)
    {
        var request = new Requests.CoordinateRequest { Node = node };

        var response = await CallAsync<Requests.CoordinateRequest, Responses.CoordinateResponse>(RpcCommands.GetCoordinate, request, expectBody: true, cancellationToken);

        return !string.IsNullOrEmpty(response.Error) ?
            throw new RpcException($"GetCoordinate command failed: {response.Error}") :
            response.Body?.Coord;
    }

    // ========== Streaming Commands ==========

    /// <summary>
    /// Opens a 'monitor' stream and returns a handle whose records are the agent's log lines.
    /// Maps to: Go's Monitor() in rpc_client.go
    /// </summary>
    public async Task<RpcStreamHandle<Responses.LogEntry>> StartMonitorAsync(string logLevel = "INFO", CancellationToken cancellationToken = default)
    {
        var request = new Requests.MonitorRequest { LogLevel = logLevel };

        var (seq, handler, init) = await OpenStreamAsync<Requests.MonitorRequest, object, Responses.LogEntry>(
            RpcCommands.Monitor, request, initHasBody: false, isTerminal: null, cancellationToken);

        if (!string.IsNullOrEmpty(init.Error))
            throw new RpcException($"Monitor command failed: {init.Error}");

        return new RpcStreamHandle<Responses.LogEntry>(this, seq, handler.Records);
    }

    /// <summary>
    /// Opens a 'stream' for the given event filter and returns a handle whose records are the events.
    /// Maps to: Go's Stream() in rpc_client.go
    /// </summary>
    public async Task<RpcStreamHandle<Responses.StreamEvent>> StartStreamAsync(string type = "*", CancellationToken cancellationToken = default)
    {
        var request = new Requests.StreamRequest { Type = type };

        var (seq, handler, init) = await OpenStreamAsync<Requests.StreamRequest, object, Responses.StreamEvent>(
            RpcCommands.Stream, request, initHasBody: false, isTerminal: null, cancellationToken);

        if (!string.IsNullOrEmpty(init.Error))
            throw new RpcException($"Stream command failed: {init.Error}");

        return new RpcStreamHandle<Responses.StreamEvent>(this, seq, handler.Records);
    }

    public async IAsyncEnumerable<string> MonitorAsync(
        string logLevel = "INFO",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var handle = await StartMonitorAsync(logLevel, cancellationToken);
        try
        {
            await foreach (var entry in EnumerateQuietlyAsync(handle.Records, cancellationToken))
            {
                yield return entry.Log;
            }
        }
        finally
        {
            await StopQuietlyAsync(handle.Seq);
        }
    }

    public async IAsyncEnumerable<Responses.StreamEvent> StreamAsync(
        string type = "*",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var handle = await StartStreamAsync(type, cancellationToken);
        try
        {
            await foreach (var streamEvent in EnumerateQuietlyAsync(handle.Records, cancellationToken))
            {
                yield return streamEvent;
            }
        }
        finally
        {
            await StopQuietlyAsync(handle.Seq);
        }
    }

    /// <summary>
    /// Stops the stream / monitor / query opened by the command with seq <paramref name="stopSeq"/>.
    /// Its local handler is removed once the agent has acknowledged (no record for that seq follows the ack).
    /// Maps to: Go's Stop() in rpc_client.go
    /// </summary>
    public async Task StopAsync(ulong stopSeq, CancellationToken cancellationToken = default)
    {
        var request = new Requests.StopRequest { Stop = stopSeq };

        try
        {
            var response = await CallAsync<Requests.StopRequest, object>(RpcCommands.Stop, request, expectBody: false, cancellationToken);

            if (!string.IsNullOrEmpty(response.Error))
                throw new RpcException($"Stop command failed: {response.Error}");
        }
        finally
        {
            ISeqHandler? handler;
            lock (_handlersLock)
            {
                _handlers.Remove(stopSeq, out handler);
            }

            handler?.Cleanup(new RpcException("Stream stopped"));
        }
    }

    /// <summary>
    /// Stops a stream, ignoring failures (already stopped, connection gone, client disposed).
    /// </summary>
    internal async Task StopQuietlyAsync(ulong stopSeq)
    {
        try
        {
            await StopAsync(stopSeq, CancellationToken.None).WaitAsync(_config.Timeout);
        }
        catch
        {
            // Best effort
        }
    }

    /// <summary>
    /// Enumerates a stream's channel; cancellation and channel closure end the enumeration quietly.
    /// </summary>
    private static async IAsyncEnumerable<TRecord> EnumerateQuietlyAsync<TRecord>(
        ChannelReader<TRecord> reader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            TRecord? record;
            try
            {
                if (!await reader.WaitToReadAsync(cancellationToken))
                {
                    yield break;
                }

                if (!reader.TryRead(out record))
                {
                    continue;
                }
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            catch (ChannelClosedException)
            {
                yield break;
            }

            yield return record;
        }
    }

    private async Task TearDownConnectionAsync()
    {
        var readerCts = _readerCts;
        var readerTask = _readerTask;
        _readerCts = null;
        _readerTask = null;

        try
        {
            readerCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already gone
        }

        _reader?.Dispose();
        if (_stream != null)
            await _stream.DisposeAsync();
        _tcpClient?.Dispose();
        _reader = null;
        _stream = null;
        _tcpClient = null;

        if (readerTask != null)
        {
            try
            {
                await readerTask.WaitAsync(TimeSpan.FromSeconds(2));
            }
            catch
            {
                // The loop swallows its own errors; only a timeout can surface here
            }
        }

        readerCts?.Dispose();
        FailPendingHandlers(new RpcException("Client closed"));
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            // Dispose managed resources; the reader loop observes the closed stream and fails pending handlers
            var readerCts = _readerCts;
            var readerTask = _readerTask;
            _readerCts = null;
            _readerTask = null;
            try
            {
                readerCts?.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Already gone
            }

            _reader?.Dispose();
            _stream?.Dispose();
            _tcpClient?.Dispose();
            FailPendingHandlers(new ObjectDisposedException(nameof(RpcClient)));
            _writeLock.Dispose();

            if (readerCts != null)
            {
                // Release the token source once the reader loop has observed the cancellation
                if (readerTask != null)
                {
                    readerTask.ContinueWith(_ => readerCts.Dispose(), CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                }
                else
                {
                    readerCts.Dispose();
                }
            }
        }

        _disposed = true;
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        if (_disposed) return;

        await TearDownConnectionAsync();
        _writeLock.Dispose();

        _disposed = true;
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        // Ensure a synchronous cleanup path is also marked as disposed
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    // ========== Response dispatch ==========

    /// <summary>
    /// A handler registered for a request seq (Go: seqHandler).
    /// </summary>
    private interface ISeqHandler
    {
        /// <summary>
        /// Whether a body follows <paramref name="header"/> on the wire and must be read before dispatching.
        /// </summary>
        bool ExpectsBody(ResponseHeader header);

        /// <summary>
        /// Handles a response; returns false once the handler must be deregistered.
        /// </summary>
        bool Handle(ResponseHeader header, ReadOnlySequence<byte>? body);

        /// <summary>
        /// Called when the handler is removed without a response (connection lost, stopped, disposed).
        /// </summary>
        void Cleanup(Exception error);
    }

    /// <summary>
    /// Completes a single response (Go: seqCallback).
    /// </summary>
    private sealed class OneShotHandler<TResponse>(bool expectBody) : ISeqHandler
    {
        private readonly TaskCompletionSource<ResponseResult<TResponse>> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<ResponseResult<TResponse>> Response => _tcs.Task;

        public bool ExpectsBody(ResponseHeader header) => expectBody && string.IsNullOrEmpty(header.Error);

        public bool Handle(ResponseHeader header, ReadOnlySequence<byte>? body)
        {
            if (!string.IsNullOrEmpty(header.Error) || body == null)
            {
                _tcs.TrySetResult(new ResponseResult<TResponse> { Error = header.Error });
                return false;
            }

            try
            {
                var decoded = MessagePackSerializer.Deserialize<TResponse>(body.Value, MsgPackOptions);
                _tcs.TrySetResult(new ResponseResult<TResponse> { Error = header.Error, Body = decoded });
            }
            catch (Exception ex)
            {
                _tcs.TrySetException(new RpcException($"Failed to decode response: {ex.Message}", ex));
            }

            return false;
        }

        public void Cleanup(Exception error) => _tcs.TrySetException(error);
    }

    /// <summary>
    /// Completes the acknowledgement of a stream-opening command, then pushes the records that follow into
    /// a channel until the stream is stopped, a terminal record arrives, or the connection is lost
    /// (Go: streamHandler / monitorHandler / queryHandler).
    /// </summary>
    private sealed class StreamHandler<TInit, TRecord>(bool initHasBody, Func<TRecord, bool>? isTerminal) : ISeqHandler
    {
        private readonly TaskCompletionSource<ResponseResult<TInit>> _init = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Channel<TRecord> _channel = Channel.CreateBounded<TRecord>(new BoundedChannelOptions(StreamBufferSize)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleWriter = true,
            SingleReader = false
        });
        private bool _initialized;

        public Task<ResponseResult<TInit>> Init => _init.Task;

        public ChannelReader<TRecord> Records => _channel.Reader;

        public bool ExpectsBody(ResponseHeader header)
            => string.IsNullOrEmpty(header.Error) && (_initialized || initHasBody);

        public bool Handle(ResponseHeader header, ReadOnlySequence<byte>? body)
        {
            if (!_initialized)
            {
                _initialized = true;
                if (!string.IsNullOrEmpty(header.Error))
                {
                    _channel.Writer.TryComplete();
                    _init.TrySetResult(new ResponseResult<TInit> { Error = header.Error });
                    return false;
                }

                TInit? initBody = default;
                if (body != null)
                {
                    try
                    {
                        initBody = MessagePackSerializer.Deserialize<TInit>(body.Value, MsgPackOptions);
                    }
                    catch (Exception ex)
                    {
                        _channel.Writer.TryComplete();
                        _init.TrySetException(new RpcException($"Failed to decode response: {ex.Message}", ex));
                        return false;
                    }
                }

                _init.TrySetResult(new ResponseResult<TInit> { Body = initBody });
                return true;
            }

            if (!string.IsNullOrEmpty(header.Error) || body == null)
            {
                _channel.Writer.TryComplete(new RpcException(string.IsNullOrEmpty(header.Error) ? "Stream ended" : header.Error));
                return false;
            }

            TRecord record;
            try
            {
                record = MessagePackSerializer.Deserialize<TRecord>(body.Value, MsgPackOptions);
            }
            catch (Exception ex)
            {
                _channel.Writer.TryComplete(new RpcException($"Failed to decode stream record: {ex.Message}", ex));
                return false;
            }

            // Dropped when the consumer is behind (Go: "Dropping ... channel full")
            _channel.Writer.TryWrite(record);

            if (isTerminal != null && isTerminal(record))
            {
                _channel.Writer.TryComplete();
                return false;
            }

            return true;
        }

        public void Cleanup(Exception error)
        {
            if (!_initialized)
            {
                _initialized = true;
                _init.TrySetException(error);
            }

            _channel.Writer.TryComplete();
        }
    }
}

/// <summary>
/// Internal result type for responses
/// </summary>
internal class ResponseResult<T>
{
    public string Error { get; set; } = string.Empty;
    public T? Body { get; set; }
}
