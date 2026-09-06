// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Text;
using NSerf.Agent;
using NSerf.Agent.RPC;
using NSerf.Client;
using Xunit;

namespace NSerfTests.Agent;

/// <summary>
/// The RPC session must multiplex commands on one connection the way Go's ipc.go does:
/// monitor/stream/query register a stream and return to the read loop, streamed records are
/// framed with the seq of the command that opened them, and stop/respond have server handlers.
/// These tests only use the pre-existing client API so they fail before the fix.
/// </summary>
public class RpcSessionMultiplexingTests
{
    private static async Task<(SerfAgent Agent, RpcServer Server, string Address)> StartAgentAsync(string nodeName)
    {
        var agent = new SerfAgent(new AgentConfig
        {
            NodeName = nodeName,
            BindAddr = "127.0.0.1:0"
        });
        await agent.StartAsync();

        var server = new RpcServer(agent, "127.0.0.1:0");
        await server.StartAsync();
        return (agent, server, server.Address!);
    }

    [Fact(Timeout = 30000)]
    public async Task Stream_ThenMembers_OnSameConnection_Succeeds()
    {
        var (agent, server, address) = await StartAgentAsync("mux-stream");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            using var cts = new CancellationTokenSource();
            var enumerator = client.StreamAsync("*", cts.Token).GetAsyncEnumerator(cts.Token);

            // MoveNextAsync sends the stream request and then waits for the first event.
            var firstEvent = enumerator.MoveNextAsync();
            await Task.Delay(500);

            // The connection must still service ordinary commands while the stream is open.
            var members = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            members.Should().ContainSingle(m => m.Name == "mux-stream");

            // And the stream must still deliver events afterwards.
            await client.UserEventAsync("deploy", Encoding.UTF8.GetBytes("v1")).WaitAsync(TimeSpan.FromSeconds(5));

            var sawUserEvent = false;
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                var moved = await firstEvent.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                moved.Should().BeTrue();
                if (enumerator.Current.Event == "user" && enumerator.Current.Name == "deploy")
                {
                    sawUserEvent = true;
                    break;
                }

                firstEvent = enumerator.MoveNextAsync();
            }

            sawUserEvent.Should().BeTrue("the user event must be delivered on the open stream");

            cts.Cancel();
            try
            {
                await firstEvent.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
                // Expected when the enumeration is cancelled mid-wait
            }

            await enumerator.DisposeAsync();
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Query_ThenMembers_OnSameConnection_Succeeds()
    {
        var (agent, server, address) = await StartAgentAsync("mux-query");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var queryId = await client.QueryAsync("ping", Encoding.UTF8.GetBytes("x"), requestAck: true, timeoutSeconds: 1);
            queryId.Should().NotBe(0UL);

            // Let the local ack record reach the client before issuing the next command.
            await Task.Delay(500);

            var members = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            members.Should().ContainSingle(m => m.Name == "mux-query");

            // After the query deadline the 'done' record must not pollute the connection either.
            await Task.Delay(1500);
            var membersAgain = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            membersAgain.Should().ContainSingle(m => m.Name == "mux-query");
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Stop_UnknownSeq_ReturnsUnknownStream()
    {
        var (agent, server, address) = await StartAgentAsync("mux-stop");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var act = async () => await client.StopAsync(424242).WaitAsync(TimeSpan.FromSeconds(5));

            (await act.Should().ThrowAsync<RpcException>())
                .WithMessage("*Unknown stream*");
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Respond_UnknownQueryId_ReturnsUnknownQuery()
    {
        var (agent, server, address) = await StartAgentAsync("mux-respond");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var act = async () => await client.RespondAsync(999999, Encoding.UTF8.GetBytes("pong")).WaitAsync(TimeSpan.FromSeconds(5));

            (await act.Should().ThrowAsync<RpcException>())
                .WithMessage("*Unknown query*");
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }
}
