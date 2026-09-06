// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net;
using System.Net.Sockets;
using MessagePack;
using NSerf.Client;
using Xunit;

namespace NSerfTests.Client;

/// <summary>
/// Cancelling a call after its request reached the agent must not desynchronise the connection: the
/// response (header and body) still arrives and has to be consumed as a unit by the reader.
/// </summary>
public class RpcClientCancellationTests
{
    /// <summary>
    /// A scripted agent: answers the handshake, serves 'join' late (after 500 ms) with a JoinResponse whose
    /// Num equals the seq the client uses for its next command, and serves 'stats' immediately.
    /// A client that dropped the join handler would read the join body as the next response header.
    /// </summary>
    private sealed class ScriptedServer : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _cts = new();

        public int Port { get; }

        public ScriptedServer()
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(AcceptAsync);
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_cts.Token);
                    _ = Task.Run(() => ServeAsync(client));
                }
            }
            catch (Exception)
            {
                // Listener stopped
            }
        }

        private async Task ServeAsync(TcpClient client)
        {
            using (client)
            {
                var stream = client.GetStream();
                using var reader = new MessagePackStreamReader(stream, leaveOpen: true);
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var headerBytes = await reader.ReadAsync(_cts.Token);
                        if (!headerBytes.HasValue) return;
                        var header = MessagePackSerializer.Deserialize<RequestHeader>(headerBytes.Value);

                        switch (header.Command)
                        {
                            case RpcCommands.Handshake:
                                await reader.ReadAsync(_cts.Token); // HandshakeRequest
                                await WriteAsync(stream, new ResponseHeader { Seq = header.Seq, Error = string.Empty });
                                break;

                            case RpcCommands.Join:
                                await reader.ReadAsync(_cts.Token); // JoinRequest
                                await Task.Delay(500, _cts.Token);
                                await WriteAsync(stream, new ResponseHeader { Seq = header.Seq, Error = string.Empty },
                                    new NSerf.Client.Responses.JoinResponse { Num = (int)header.Seq + 1 });
                                break;

                            case RpcCommands.Stats:
                                await WriteAsync(stream, new ResponseHeader { Seq = header.Seq, Error = string.Empty },
                                    new NSerf.Client.Responses.StatsResponse
                                    {
                                        Stats = new Dictionary<string, Dictionary<string, string>>
                                        {
                                            ["agent"] = new() { ["name"] = "scripted" }
                                        }
                                    });
                                break;

                            default:
                                await WriteAsync(stream, new ResponseHeader { Seq = header.Seq, Error = "Unknown command" });
                                break;
                        }
                    }
                }
                catch (Exception)
                {
                    // Connection closed
                }
            }
        }

        private async Task WriteAsync(NetworkStream stream, ResponseHeader header)
        {
            await stream.WriteAsync(MessagePackSerializer.Serialize(header), _cts.Token);
            await stream.FlushAsync(_cts.Token);
        }

        private async Task WriteAsync<TBody>(NetworkStream stream, ResponseHeader header, TBody body)
        {
            await stream.WriteAsync(MessagePackSerializer.Serialize(header), _cts.Token);
            await stream.WriteAsync(MessagePackSerializer.Serialize(body), _cts.Token);
            await stream.FlushAsync(_cts.Token);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
            _cts.Dispose();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task CancelledCall_AfterRequestWasSent_DoesNotDesynchroniseTheConnection()
    {
        using var server = new ScriptedServer();
        await using var client = new RpcClient(new RpcConfig
        {
            Address = $"127.0.0.1:{server.Port}",
            Timeout = TimeSpan.FromSeconds(5)
        });
        await client.ConnectAsync();

        // The join is answered late; the caller gives up first
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var join = async () => await client.JoinAsync(["10.0.0.1:7946"], cancellationToken: cts.Token);
        await join.Should().ThrowAsync<OperationCanceledException>();

        // The late join response (header + body) arrives before the stats response and must be consumed
        // as a unit; a client that forgot the join's seq would parse the body as the stats header.
        var stats = await client.StatsAsync();
        stats["agent"]["name"].Should().Be("scripted");
    }
}
