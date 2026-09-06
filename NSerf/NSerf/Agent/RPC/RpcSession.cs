// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net.Sockets;
using System.Runtime.InteropServices;
using MessagePack;
using NSerf.Client;
using NSerf.Serf;
using NSerf.Serf.Events;

namespace NSerf.Agent.RPC;

public class RpcSession : IAsyncDisposable
{
    private readonly SerfAgent _agent;
    private readonly TcpClient _client;
    private readonly string? _authKey;
    private readonly NetworkStream? _stream;
    private readonly MessagePackStreamReader? _reader;
    private bool _authenticated;
    private bool _disposed;
    private int _clientVersion;  // 0 = no handshake yet
    private readonly SemaphoreSlim _writeLock = new(1, 1);  // CRITICAL: Prevent overlapping writes

    // Streams opened on this connection keyed by the seq of the command that opened them
    // (Go: IPCClient.eventStreams / logStreamer / queryStreams). Disposing an entry stops it.
    private readonly Dictionary<ulong, IDisposable> _streams = [];
    private readonly object _streamsLock = new();

    // Queries streamed to this client that it may still answer with 'respond'
    // (Go: IPCClient.pendingQueries, keyed by a per-session counter).
    private readonly Dictionary<ulong, Query> _pendingQueries = [];
    private readonly object _pendingQueriesLock = new();
    private ulong _nextQueryId;

    // Cancelled when the session ends so timers and stream writers stop
    private readonly CancellationTokenSource _sessionCts = new();

    private static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.None);

    public RpcSession(SerfAgent agent, TcpClient client, string? authKey)
    {
        _agent = agent;
        _client = client;
        _authKey = authKey;
        _stream = client.GetStream();
        _reader = new MessagePackStreamReader(_stream, leaveOpen: true);
    }

    public async Task HandleAsync(CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _sessionCts.Token);
        cancellationToken = linkedCts.Token;

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed && _reader != null)
            {
                RequestHeader? header;
                try
                {
                    var headerBytes = await _reader.ReadAsync(cancellationToken);
                    if (!headerBytes.HasValue)
                    {
                        break;
                    }

                    header = MessagePackSerializer.Deserialize<RequestHeader>(headerBytes.Value, MsgPackOptions, cancellationToken);
                }
                catch (IOException ex)
                {
                    // Windows throws WSA errors on EOF - don't log as errors
                    if (!IsWindowsSocketClosed(ex))
                    {
                        // Actual IO error, not normal disconnect
                    }
                    break;
                }
                catch (SocketException)
                {
                    // Normal disconnect
                    break;
                }
                catch (MessagePackSerializationException)
                {
                    // Malformed message
                    break;
                }
                catch (Exception)
                {
                    break;
                }

                await HandleCommandAsync(header, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Connection closed
        }
        catch (Exception)
        {
            // Session error
        }
        finally
        {
            // The connection is gone: stop every stream and forget pending queries (Go: deregisters on close)
            CancelSession();
            StopAllStreams();
        }
    }

    private async Task HandleCommandAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        try
        {
            // Ensure a handshake is performed before other commands
            if (header.Command != RpcCommands.Handshake && _clientVersion == 0)
            {
                await SendErrorAsync(header.Seq, "Handshake required", cancellationToken);
                return;
            }

            // Ensure a client has authenticated after handshake if necessary
            if (!string.IsNullOrEmpty(_authKey) && !_authenticated &&
                header.Command != RpcCommands.Auth && header.Command != RpcCommands.Handshake)
            {
                await SendErrorAsync(header.Seq, "Authentication required", cancellationToken);
                return;
            }

            switch (header.Command)
            {
                case RpcCommands.Handshake:
                    await HandleHandshakeAsync(header, cancellationToken);
                    break;

                case RpcCommands.Auth:
                    await HandleAuthAsync(header, cancellationToken);
                    break;

                case RpcCommands.Members:
                    await HandleMembersAsync(header, cancellationToken);
                    break;

                case RpcCommands.Join:
                    await HandleJoinAsync(header, cancellationToken);
                    break;

                case RpcCommands.Leave:
                    await HandleLeaveAsync(header, cancellationToken);
                    break;

                case RpcCommands.MembersFiltered:
                    await HandleMembersFilteredAsync(header, cancellationToken);
                    break;

                case RpcCommands.ForceLeave:
                    await HandleForceLeaveAsync(header, cancellationToken);
                    break;

                case RpcCommands.Event:
                    await HandleUserEventAsync(header, cancellationToken);
                    break;

                case RpcCommands.Tags:
                    await HandleTagsAsync(header, cancellationToken);
                    break;

                case RpcCommands.Query:
                    await HandleQueryAsync(header, cancellationToken);
                    break;

                case RpcCommands.Stats:
                    await HandleStatsAsync(header, cancellationToken);
                    break;

                case RpcCommands.GetCoordinate:
                    await HandleGetCoordinateAsync(header, cancellationToken);
                    break;

                case RpcCommands.Monitor:
                    await HandleMonitorAsync(header, cancellationToken);
                    break;

                case RpcCommands.Stream:
                    await HandleStreamAsync(header, cancellationToken);
                    break;

                case RpcCommands.Stop:
                    await HandleStopAsync(header, cancellationToken);
                    break;

                case RpcCommands.Respond:
                    await HandleRespondAsync(header, cancellationToken);
                    break;

                case RpcCommands.InstallKey:
                    await HandleKeyCommandAsync(header, cancellationToken, readKey: true, (km, key) => km.InstallKey(key));
                    break;

                case RpcCommands.UseKey:
                    await HandleKeyCommandAsync(header, cancellationToken, readKey: true, (km, key) => km.UseKey(key));
                    break;

                case RpcCommands.RemoveKey:
                    await HandleKeyCommandAsync(header, cancellationToken, readKey: true, (km, key) => km.RemoveKey(key));
                    break;

                case RpcCommands.ListKeys:
                    await HandleKeyCommandAsync(header, cancellationToken, readKey: false, (km, _) => km.ListKeys());
                    break;

                default:
                    await SendErrorAsync(header.Seq, $"Unknown command: {header.Command}", cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            await SendErrorAsync(header.Seq, ex.Message, cancellationToken);
        }
    }

    private async Task HandleHandshakeAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<HandshakeRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        // Check for duplicate handshake
        if (_clientVersion != 0)
        {
            await SendErrorAsync(header.Seq, "Duplicate handshake", cancellationToken);
            return;
        }

        var response = new ResponseHeader
        {
            Seq = header.Seq,
            Error = request.Version > RpcConstants.MaxIpcVersion
                ? $"Unsupported version: {request.Version}"
                : string.Empty
        };

        // Store client version if handshake successful
        if (string.IsNullOrEmpty(response.Error))
        {
            _clientVersion = request.Version;
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleAuthAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<AuthRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        var error = string.Empty;
        if (!string.IsNullOrEmpty(_authKey) && request.AuthKey != _authKey)
        {
            error = "Invalid auth key";
        }
        else
        {
            _authenticated = true;
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = error };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleMembersAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var members = _agent.Serf?.Members() ?? [];

        var rpcMembers = members.Select(m => new Client.Responses.Member
        {
            Name = m.Name,
            Addr = m.Addr.GetAddressBytes(),
            Port = m.Port,
            Tags = m.Tags,
            Status = m.Status.ToString().ToLowerInvariant(),
            ProtocolMin = m.ProtocolMin,
            ProtocolMax = m.ProtocolMax,
            ProtocolCur = m.ProtocolCur,
            DelegateMin = m.DelegateMin,
            DelegateMax = m.DelegateMax,
            DelegateCur = m.DelegateCur
        }).ToArray();

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var membersResponse = new Client.Responses.MembersResponse { Members = rpcMembers };
            var bodyBytes = MessagePackSerializer.Serialize(membersResponse, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleJoinAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.JoinRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        var joined = await _agent.Serf!.JoinAsync(request.Existing, request.Replay);

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var joinResponse = new Client.Responses.JoinResponse { Num = joined };
            var bodyBytes = MessagePackSerializer.Serialize(joinResponse, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleLeaveAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        await _agent.Serf!.LeaveAsync();

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleMembersFilteredAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.MembersFilteredRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        var allMembers = _agent.Serf?.Members() ?? [];

        // Pre-compile regex patterns with anchors (^$) for an exact match
        System.Text.RegularExpressions.Regex? nameRegex = null;
        System.Text.RegularExpressions.Regex? statusRegex = null;
        Dictionary<string, System.Text.RegularExpressions.Regex>? tagRegexes = null;

        try
        {
            if (!string.IsNullOrEmpty(request.Name))
                nameRegex = new System.Text.RegularExpressions.Regex($"^{request.Name}$");

            if (!string.IsNullOrEmpty(request.Status))
                statusRegex = new System.Text.RegularExpressions.Regex($"^{request.Status}$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

            if (request.Tags.Count > 0)
            {
                tagRegexes = [];
                foreach (var tag in request.Tags)
                {
                    tagRegexes[tag.Key] = new System.Text.RegularExpressions.Regex($"^{tag.Value}$");
                }
            }
        }
        catch (ArgumentException ex)
        {
            await SendErrorAsync(header.Seq, $"Invalid regex pattern: {ex.Message}", cancellationToken);
            return;
        }

        var filtered = allMembers.Where(m =>
        {
            if (statusRegex != null && !statusRegex.IsMatch(m.Status.ToString().ToLowerInvariant()))
                return false;

            if (nameRegex != null && !nameRegex.IsMatch(m.Name))
                return false;

            if (tagRegexes == null) return true;

            foreach (var tagRegex in tagRegexes)
            {
                if (!m.Tags.TryGetValue(tagRegex.Key, out var value) || !tagRegex.Value.IsMatch(value))
                    return false;
            }
            return true;
        }).ToArray();

        var rpcMembers = filtered.Select(m => new Client.Responses.Member
        {
            Name = m.Name,
            Addr = m.Addr.GetAddressBytes(),
            Port = m.Port,
            Tags = m.Tags,
            Status = m.Status.ToString().ToLowerInvariant(),
            ProtocolMin = m.ProtocolMin,
            ProtocolMax = m.ProtocolMax,
            ProtocolCur = m.ProtocolCur,
            DelegateMin = m.DelegateMin,
            DelegateMax = m.DelegateMax,
            DelegateCur = m.DelegateCur
        }).ToArray();

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var membersResponse = new Client.Responses.MembersResponse { Members = rpcMembers };
            var bodyBytes = MessagePackSerializer.Serialize(membersResponse, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleForceLeaveAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.ForceLeaveRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        await _agent.Serf!.RemoveFailedNodeAsync(request.Node);

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleUserEventAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.EventRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        await _agent.Serf!.UserEventAsync(request.Name, request.Payload, request.Coalesce);

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleTagsAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.TagsRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        // Merge existing tags with new tags, excluding deleted tags
        var mergedTags = new Dictionary<string, string>();

        // Start with existing tags
        var currentTags = _agent.Serf?.Config.Tags ?? [];

        foreach (var tag in currentTags
                     .Where(tag => !request.DeleteTags.Contains(tag.Key)))
        {
            mergedTags[tag.Key] = tag.Value;
        }

        // Add/update with new tags
        foreach (var tag in request.Tags)
        {
            mergedTags[tag.Key] = tag.Value;
        }

        // Apply the merged tags
        await _agent.SetTagsAsync(mergedTags);

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleQueryAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.QueryRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        var queryParam = new QueryParam
        {
            FilterNodes = request.FilterNodes.Length > 0 ? request.FilterNodes : null,
            FilterTags = request.FilterTags.Count > 0 ? request.FilterTags : null,
            RequestAck = request.RequestAck,
            Timeout = TimeSpan.FromSeconds(request.Timeout)
        };

        // Start the query
        QueryResponse? queryResp = null;
        string? errorMsg = null;
        try
        {
            queryResp = await _agent.Serf!.QueryAsync(request.Name, request.Payload, queryParam);
        }
        catch (Exception ex)
        {
            errorMsg = ex.Message;
        }

        // An error response is header-only, like every other command (Go: no body on error)
        if (queryResp == null)
        {
            await SendErrorAsync(header.Seq, errorMsg ?? "Query failed", cancellationToken);
            return;
        }

        // Register the response stream before acknowledging so a stop cannot race the registration
        var streamer = new QueryResponseStream(_writeLock, _stream!, header.Seq, queryResp);
        RegisterStream(header.Seq, streamer);

        // Send initial response with query ID
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var queryResponse = new Client.Responses.QueryResponse { Id = queryResp.Id };
            var bodyBytes = MessagePackSerializer.Serialize(queryResponse, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }

        // Stream the query records (ack / response / done) asynchronously (Go: defer qs.Stream)
        _ = Task.Run(async () =>
        {
            try
            {
                await streamer.StreamAsync(cancellationToken);
            }
            finally
            {
                UnregisterStream(header.Seq, streamer);
            }
        }, CancellationToken.None);
    }

    private async Task HandleStatsAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var stats = new Dictionary<string, Dictionary<string, string>>
        {
            ["agent"] = new()
            {
                ["name"] = _agent.Serf?.Config.NodeName ?? "unknown"
            },
            ["serf"] = new()
            {
                ["members"] = _agent.Serf?.NumMembers().ToString() ?? "0",
                ["event_time"] = _agent.Serf?.EventClock.Time().ToString() ?? "0",
                ["query_time"] = _agent.Serf?.QueryClock.Time().ToString() ?? "0"
            },
            ["runtime"] = new()
            {
                ["os"] = Environment.OSVersion.Platform.ToString(),
                ["arch"] = RuntimeInformation.ProcessArchitecture.ToString()
            }
        };

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var statsResponse = new Client.Responses.StatsResponse { Stats = stats };
            var bodyBytes = MessagePackSerializer.Serialize(statsResponse, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleGetCoordinateAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue)
        {
            await SendErrorAsync(header.Seq, "Failed to read coordinate request", cancellationToken);
            return;
        }

        Client.Requests.CoordinateRequest? request;
        try
        {
            request = MessagePackSerializer.Deserialize<Client.Requests.CoordinateRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);
        }
        catch (Exception ex)
        {
            await SendErrorAsync(header.Seq, $"Failed to decode coordinate request: {ex.Message}", cancellationToken);
            return;
        }

        if (string.IsNullOrEmpty(request.Node))
        {
            await SendErrorAsync(header.Seq, "Node name is required", cancellationToken);
            return;
        }

        var coord = _agent.Serf?.GetCachedCoordinate(request.Node);

        var response = new Client.Responses.CoordinateResponse
        {
            Ok = coord != null,
            Coord = coord != null ? new Client.Responses.Coordinate
            {
                Vec = [.. coord.Vec.Select(v => (float)v)],
                Error = (float)coord.Error,
                Adjustment = (float)coord.Adjustment,
                Height = (float)coord.Height
            } : null
        };

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var headerResponse = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var responseBytes = MessagePackSerializer.Serialize(headerResponse, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(responseBytes, cancellationToken);

            var bodyBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task HandleMonitorAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        // Read monitor request
        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue)
        {
            await SendErrorAsync(header.Seq, "Failed to read monitor request", cancellationToken);
            return;
        }

        var request = MessagePackSerializer.Deserialize<Client.Requests.MonitorRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        // Apply log-level filtering as requested by the client; records carry the seq of this command
        var requestedLevel = LogLevelExtensions.FromString(request.LogLevel ?? "INFO");
        var baseHandler = new RpcLogHandler(_stream!, _writeLock, header.Seq, cancellationToken);
        var filteredHandler = new FilteredLogHandler(baseHandler, requestedLevel);
        RegisterStream(header.Seq, new LogStreamRegistration(_agent.LogWriter, filteredHandler, baseHandler));

        // Acknowledge first, then register so the backlog and live lines follow the ack
        // (Go: defer i.logWriter.RegisterHandler(client.logStreamer)); the read loop continues immediately.
        await SendHeaderAsync(header.Seq, string.Empty, cancellationToken);
        _agent.LogWriter?.RegisterHandler(filteredHandler);
    }

    private async Task HandleStreamAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        // Read stream request
        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue)
        {
            await SendErrorAsync(header.Seq, "Failed to read stream request", cancellationToken);
            return;
        }

        var request = MessagePackSerializer.Deserialize<Client.Requests.StreamRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        // Validate the filter before acknowledging (Go: "Unknown event filter")
        RpcEventHandler eventHandler;
        try
        {
            eventHandler = new RpcEventHandler(_stream!, _writeLock, header.Seq, request.Type, RegisterQuery, cancellationToken);
        }
        catch (ArgumentException ex)
        {
            await SendErrorAsync(header.Seq, $"Invalid event filter '{request.Type}': {ex.Message}", cancellationToken);
            return;
        }

        RegisterStream(header.Seq, new EventStreamRegistration(_agent, eventHandler));

        // Acknowledge first, then register so events follow the ack
        // (Go: defer i.agent.RegisterEventHandler(...)); the read loop continues immediately.
        await SendHeaderAsync(header.Seq, string.Empty, cancellationToken);
        _agent.RegisterEventHandler(eventHandler);
    }

    /// <summary>
    /// Stops the stream (monitor, stream or query) opened by the command with seq = request.Stop.
    /// Maps to: Go's handleStop in ipc.go (NSerf reports an unknown seq as an error).
    /// </summary>
    private async Task HandleStopAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.StopRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        IDisposable? stream;
        lock (_streamsLock)
        {
            _streams.Remove(request.Stop, out stream);
        }

        if (stream == null)
        {
            await SendErrorAsync(header.Seq, "Unknown stream", cancellationToken);
            return;
        }

        // Deregister and stop before acknowledging: no record for that seq follows the ack
        stream.Dispose();
        await SendHeaderAsync(header.Seq, string.Empty, cancellationToken);
    }

    /// <summary>
    /// Answers a query that was streamed to this client, identified by the session-scoped query ID.
    /// Maps to: Go's handleRespond in ipc.go
    /// </summary>
    private async Task HandleRespondAsync(RequestHeader header, CancellationToken cancellationToken)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var requestBytes = await _reader!.ReadAsync(cancellationToken);
        if (!requestBytes.HasValue) return;

        var request = MessagePackSerializer.Deserialize<Client.Requests.RespondRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);

        Query? query;
        lock (_pendingQueriesLock)
        {
            _pendingQueries.TryGetValue(request.ID, out query);
        }

        var error = string.Empty;
        if (query == null)
        {
            error = "Unknown query";
        }
        else
        {
            try
            {
                await query.RespondAsync(request.Payload);
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }
        }

        await SendHeaderAsync(header.Seq, error, cancellationToken);
    }

    /// <summary>
    /// Registers a streamed query as pending so the client can answer it with 'respond', and forgets it
    /// once its deadline passes. Maps to: Go's IPCClient.RegisterQuery in ipc.go
    /// </summary>
    private ulong RegisterQuery(Query query)
    {
        ulong id;
        lock (_pendingQueriesLock)
        {
            id = _nextQueryId++;
            _pendingQueries[id] = query;
        }

        var timeout = query.GetDeadline() - DateTime.UtcNow;
        if (timeout < TimeSpan.Zero)
        {
            timeout = TimeSpan.Zero;
        }

        // Task.Delay rejects delays beyond ~49 days; a query with an absurd timeout is simply kept that long
        var maxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
        if (timeout > maxDelay)
        {
            timeout = maxDelay;
        }

        _ = Task.Delay(timeout, _sessionCts.Token).ContinueWith(_ =>
        {
            lock (_pendingQueriesLock)
            {
                _pendingQueries.Remove(id);
            }
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        return id;
    }

    private void RegisterStream(ulong seq, IDisposable stream)
    {
        IDisposable? previous;
        lock (_streamsLock)
        {
            _streams.Remove(seq, out previous);
            _streams[seq] = stream;
        }

        // A client that reuses a seq replaces the stream it opened with it
        previous?.Dispose();
    }

    private void UnregisterStream(ulong seq, IDisposable stream)
    {
        lock (_streamsLock)
        {
            if (_streams.TryGetValue(seq, out var current) && ReferenceEquals(current, stream))
            {
                _streams.Remove(seq);
            }
        }
    }

    private void StopAllStreams()
    {
        IDisposable[] streams;
        lock (_streamsLock)
        {
            streams = [.. _streams.Values];
            _streams.Clear();
        }

        foreach (var stream in streams)
        {
            try
            {
                stream.Dispose();
            }
            catch
            {
                // Best effort teardown
            }
        }

        lock (_pendingQueriesLock)
        {
            _pendingQueries.Clear();
        }
    }

    private void CancelSession()
    {
        try
        {
            _sessionCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already torn down
        }
    }

    /// <summary>
    /// An open 'stream' command: disposing deregisters the handler from the agent and stops its writes.
    /// </summary>
    private sealed class EventStreamRegistration(SerfAgent agent, RpcEventHandler handler) : IDisposable
    {
        public void Dispose()
        {
            agent.DeregisterEventHandler(handler);
            handler.Dispose();
        }
    }

    /// <summary>
    /// An open 'monitor' command: disposing deregisters the handler from the log writer and stops its writes.
    /// </summary>
    private sealed class LogStreamRegistration(CircularLogWriter? logWriter, CircularLogWriter.ILogHandler filtered, RpcLogHandler handler) : IDisposable
    {
        public void Dispose()
        {
            logWriter?.DeregisterHandler(filtered);
            handler.Dispose();
        }
    }

    /// <summary>
    /// Handles install-key / use-key / remove-key / list-keys by delegating to <see cref="KeyManager"/>.
    /// Per-node failures are reported through the response body (NumErr / Messages), not the header.
    /// Maps to: Go's handleInstallKey/handleUseKey/handleRemoveKey/handleListKeys in ipc.go
    /// </summary>
    private async Task HandleKeyCommandAsync(
        RequestHeader header,
        CancellationToken cancellationToken,
        bool readKey,
        Func<KeyManager, string, Task<KeyResponse>> operation)
    {
        if (!CheckAuth())
        {
            await SendErrorAsync(header.Seq, "Not authenticated", cancellationToken);
            return;
        }

        var key = string.Empty;
        if (readKey)
        {
            var requestBytes = await _reader!.ReadAsync(cancellationToken);
            if (!requestBytes.HasValue) return;

            var request = MessagePackSerializer.Deserialize<Client.Requests.KeyRequest>(requestBytes.Value, MsgPackOptions, cancellationToken);
            key = request.Key;
        }

        var serf = _agent.Serf ?? throw new InvalidOperationException("Agent not started");
        var result = await operation(new KeyManager(serf), key);

        var response = new Client.Responses.KeyResponse
        {
            NumNodes = result.NumNodes,
            NumErr = result.NumErr,
            NumResp = result.NumResp,
            Keys = result.Keys,
            Messages = result.Messages,
            PrimaryKeys = result.PrimaryKeys
        };

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var headerResponse = new ResponseHeader { Seq = header.Seq, Error = string.Empty };
            var headerBytes = MessagePackSerializer.Serialize(headerResponse, MsgPackOptions, cancellationToken);
            await _stream!.WriteAsync(headerBytes, cancellationToken);

            var bodyBytes = MessagePackSerializer.Serialize(response, MsgPackOptions, cancellationToken);
            await _stream.WriteAsync(bodyBytes, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private Task SendErrorAsync(ulong seq, string error, CancellationToken cancellationToken)
        => SendHeaderAsync(seq, error, cancellationToken);

    /// <summary>
    /// Writes a header-only response (an acknowledgement when <paramref name="error"/> is empty).
    /// </summary>
    private async Task SendHeaderAsync(ulong seq, string error, CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            var response = new ResponseHeader { Seq = seq, Error = error };
            await MessagePackSerializer.SerializeAsync(_stream!, response, MsgPackOptions, cancellationToken);
            await _stream!.FlushAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private bool CheckAuth()
    {
        return string.IsNullOrEmpty(_authKey) || _authenticated;
    }

    /// <summary>
    /// Checks if an IOException is a Windows socket closed error (WSARECV).
    /// Windows throws "WSARECV" errors on EOF, which are normal disconnects.
    /// </summary>
    private static bool IsWindowsSocketClosed(IOException ex)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
            ex.Message.Contains("WSA", StringComparison.OrdinalIgnoreCase);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        // Stop stream writers and pending-query timers before tearing down the connection
        CancelSession();
        StopAllStreams();

        _reader?.Dispose();
        _stream?.Dispose();
        _client.Dispose();
        _writeLock.Dispose();
        _sessionCts.Dispose();

        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
