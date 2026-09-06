// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Text;
using NSerf.Agent;
using NSerf.Agent.RPC;
using NSerf.Client;
using NSerf.Client.Responses;
using Xunit;

namespace NSerfTests.Agent;

/// <summary>
/// Stream / monitor / query handles: records are demultiplexed by seq, 'stop' ends a stream server-side,
/// and a streamed query can be answered with 'respond' (Go: rpc_client_test.go TestRPCClientStream*,
/// TestRPCClientMonitor, TestRPCClientQuery, TestRPCClientStreamQuery).
/// </summary>
public class RpcStreamHandleTests
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

    private static int RegisteredEventHandlerCount(SerfAgent agent)
    {
        var field = typeof(SerfAgent).GetField("_eventHandlers",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var handlers = (HashSet<IEventHandler>)field!.GetValue(agent)!;
        lock (handlers)
        {
            return handlers.Count;
        }
    }

    private static async Task<TRecord> ReadUntilAsync<TRecord>(
        System.Threading.Channels.ChannelReader<TRecord> records,
        Func<TRecord, bool> predicate,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        await foreach (var record in records.ReadAllAsync(cts.Token))
        {
            if (predicate(record))
            {
                return record;
            }
        }

        throw new Xunit.Sdk.XunitException("The stream completed before the expected record arrived");
    }

    [Fact(Timeout = 30000)]
    public async Task Stop_EndsStream_AndLaterEventsAreNotDelivered()
    {
        var (agent, server, address) = await StartAgentAsync("handle-stop");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var baseline = RegisteredEventHandlerCount(agent);

            var stream = await client.StartStreamAsync("user");
            stream.Seq.Should().NotBe(0UL);

            // The agent acknowledges before it registers the handler (Go: deferred RegisterEventHandler), so poll
            await NSerfTests.Serf.TestHelpers.WaitForConditionAsync(
                () => RegisteredEventHandlerCount(agent) == baseline + 1,
                TimeSpan.FromSeconds(5),
                () => $"the stream's event handler was not registered on the agent (count {RegisteredEventHandlerCount(agent)}, baseline {baseline})");

            await client.UserEventAsync("first", Encoding.UTF8.GetBytes("1"));
            var first = await ReadUntilAsync(stream.Records, e => e.Name == "first", TimeSpan.FromSeconds(5));
            first.Event.Should().Be("user");
            Encoding.UTF8.GetString(first.Payload).Should().Be("1");

            await stream.StopAsync();

            // The server deregistered the handler and the client completed the channel
            RegisteredEventHandlerCount(agent).Should().Be(baseline);
            await stream.Completion.WaitAsync(TimeSpan.FromSeconds(5));

            await client.UserEventAsync("second", Encoding.UTF8.GetBytes("2"));
            await Task.Delay(500);
            stream.Records.TryRead(out _).Should().BeFalse("no event may be delivered after stop");

            // The connection stays usable, and stopping twice reports the unknown stream
            var members = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            members.Should().ContainSingle();

            var act = async () => await stream.StopAsync();
            (await act.Should().ThrowAsync<RpcException>()).WithMessage("*Unknown stream*");
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Monitor_DeliversLogRecords_AndStopWorks()
    {
        var (agent, server, address) = await StartAgentAsync("handle-monitor");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var monitor = await client.StartMonitorAsync("DEBUG");

            agent.LogWriter!.WriteLine("[INFO] hello-from-monitor-test");
            var line = await ReadUntilAsync(monitor.Records, l => l.Log.Contains("hello-from-monitor-test"), TimeSpan.FromSeconds(5));
            line.Log.Should().Contain("hello-from-monitor-test");

            // Ordinary commands keep working while the monitor is open
            var members = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            members.Should().ContainSingle();

            await monitor.StopAsync();
            await monitor.Completion.WaitAsync(TimeSpan.FromSeconds(5));

            agent.LogWriter.WriteLine("[INFO] after-stop");
            await Task.Delay(300);

            // Nothing written after the stop reaches the client, and the connection is still usable
            while (monitor.Records.TryRead(out var stale))
            {
                stale.Log.Should().NotContain("after-stop");
            }

            var stats = await client.StatsAsync().WaitAsync(TimeSpan.FromSeconds(5));
            stats["agent"]["name"].Should().Be("handle-monitor");
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task QueryHandle_ReceivesLocalAck_ThenDone()
    {
        var (agent, server, address) = await StartAgentAsync("handle-query");
        try
        {
            await using var client = new RpcClient(new RpcConfig { Address = address });
            await client.ConnectAsync();

            var query = await client.StartQueryAsync("ping", Encoding.UTF8.GetBytes("x"), requestAck: true, timeoutSeconds: 1);
            query.Id.Should().NotBe(0UL);
            query.Seq.Should().NotBe(0UL);

            var records = new List<QueryRecord>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var record in query.ReadAllAsync(cts.Token))
            {
                records.Add(record);
            }

            records.Should().Contain(r => r.Type == QueryRecordType.Ack && r.From == "handle-query");
            records.Last().Type.Should().Be(QueryRecordType.Done);
            query.Completion.IsCompletedSuccessfully.Should().BeTrue();

            // Records were consumed by the handler, so the connection is clean
            var members = await client.MembersAsync().WaitAsync(TimeSpan.FromSeconds(5));
            members.Should().ContainSingle();
        }
        finally
        {
            await server.DisposeAsync();
            await agent.DisposeAsync();
        }
    }

    [Fact(Timeout = 60000)]
    public async Task Respond_AnswersStreamedQuery_EndToEnd()
    {
        var (agentA, serverA, addressA) = await StartAgentAsync("respond-a");
        var (agentB, serverB, addressB) = await StartAgentAsync("respond-b");
        try
        {
            var memberA = agentA.Serf!.Members().Single(m => m.Name == "respond-a");
            await agentB.Serf!.JoinAsync([$"{memberA.Addr}:{memberA.Port}"], ignoreOld: true);

            var joinDeadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < joinDeadline && (agentA.Serf.NumMembers() < 2 || agentB.Serf.NumMembers() < 2))
            {
                await Task.Delay(100);
            }

            agentA.Serf.NumMembers().Should().Be(2);
            agentB.Serf.NumMembers().Should().Be(2);

            await using var clientA = new RpcClient(new RpcConfig { Address = addressA });
            await clientA.ConnectAsync();
            await using var clientB = new RpcClient(new RpcConfig { Address = addressB });
            await clientB.ConnectAsync();

            // B listens for query events on its own agent
            var streamB = await clientB.StartStreamAsync("query");

            // A issues a query
            var query = await clientA.StartQueryAsync("ping", Encoding.UTF8.GetBytes("who"), requestAck: true, timeoutSeconds: 5);

            // B sees the query, with a session-scoped ID, and answers it through 'respond'
            var queryEvent = await ReadUntilAsync(streamB.Records, e => e.Event == "query" && e.Name == "ping", TimeSpan.FromSeconds(10));
            Encoding.UTF8.GetString(queryEvent.Payload).Should().Be("who");
            queryEvent.From.Should().Be("respond-a");

            await clientB.RespondAsync(queryEvent.QueryID, Encoding.UTF8.GetBytes("pong-from-b"));

            // A's query handle receives B's ack and response
            var response = await ReadUntilAsync(query.Records,
                r => r.Type == QueryRecordType.Response && r.From == "respond-b",
                TimeSpan.FromSeconds(10));
            Encoding.UTF8.GetString(response.Payload).Should().Be("pong-from-b");

            // Answering twice is rejected with the query's own error
            var again = async () => await clientB.RespondAsync(queryEvent.QueryID, Encoding.UTF8.GetBytes("dup"));
            (await again.Should().ThrowAsync<RpcException>()).WithMessage("*Response already sent*");

            await streamB.StopAsync();
        }
        finally
        {
            await serverB.DisposeAsync();
            await agentB.DisposeAsync();
            await serverA.DisposeAsync();
            await agentA.DisposeAsync();
        }
    }
}
