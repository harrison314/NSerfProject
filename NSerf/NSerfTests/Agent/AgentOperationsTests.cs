// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using NSerf.Agent;
using Xunit;

namespace NSerfTests.Agent;

public class AgentOperationsTests
{
    [Fact]
    public async Task Agent_Join_DelegatesToSerf()
    {
        var config1 = new AgentConfig
        {
            NodeName = "node1",
            BindAddr = "127.0.0.1:19946"
        };

        var config2 = new AgentConfig
        {
            NodeName = "node2",
            BindAddr = "127.0.0.1:19947"
        };

        var agent1 = new SerfAgent(config1);
        var agent2 = new SerfAgent(config2);

        await agent1.StartAsync();
        await agent2.StartAsync();

        var count = await agent2.Serf!.JoinAsync(new[] { "127.0.0.1:19946" }, ignoreOld: true);

        Assert.Equal(1, count);

        await agent2.DisposeAsync();
        await agent1.DisposeAsync();
    }

    [Fact]
    public async Task Agent_JoinReturnsCount()
    {
        var config = new AgentConfig
        {
            NodeName = "test-join-count",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        // Try to join non-existent nodes - should throw
        await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await agent.Serf!.JoinAsync(new[] { "127.0.0.1:19999" }, ignoreOld: true);
        });

        await agent.DisposeAsync();
    }

    [Fact]
    public async Task Agent_UserEvent_Broadcasts()
    {
        // Two agents: the event sent by one must be delivered to the other's event handlers
        var sender = new SerfAgent(new AgentConfig { NodeName = "test-event", BindAddr = "127.0.0.1:0" });
        var receiver = new SerfAgent(new AgentConfig { NodeName = "test-event-receiver", BindAddr = "127.0.0.1:0" });
        await sender.StartAsync();
        await receiver.StartAsync();

        var received = new TestEventHandler();
        receiver.RegisterEventHandler(received);

        var senderMember = sender.Serf!.LocalMember();
        var joined = await receiver.Serf!.JoinAsync(new[] { $"{senderMember.Addr}:{senderMember.Port}" }, ignoreOld: true);
        Assert.Equal(1, joined);
        await NSerfTests.Serf.TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), sender.Serf, receiver.Serf);

        var payload = System.Text.Encoding.UTF8.GetBytes("test payload");
        await sender.Serf.UserEventAsync("test-event", payload, coalesce: false);

        // The receiving agent's handlers see the user event
        await NSerfTests.Serf.TestHelpers.WaitForConditionAsync(
            () =>
            {
                lock (received.ReceivedEvents)
                {
                    return received.ReceivedEvents.OfType<NSerf.Serf.Events.UserEvent>().Any(e => e.Name == "test-event");
                }
            },
            TimeSpan.FromSeconds(10),
            () => $"the receiver never got the user event; it received {received.Count} event(s)");

        NSerf.Serf.Events.UserEvent userEvent;
        lock (received.ReceivedEvents)
        {
            userEvent = received.ReceivedEvents.OfType<NSerf.Serf.Events.UserEvent>().First(e => e.Name == "test-event");
        }
        Assert.Equal(payload, userEvent.Payload);

        await receiver.DisposeAsync();
        await sender.DisposeAsync();
    }

    [Fact]
    public async Task Agent_Query_InitiatesQuery()
    {
        var config = new AgentConfig
        {
            NodeName = "test-query",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        var payload = System.Text.Encoding.UTF8.GetBytes("query data");
        var queryParam = new NSerf.Serf.QueryParam
        {
            FilterNodes = Array.Empty<string>(),
            FilterTags = new Dictionary<string, string>(),
            RequestAck = false,
            Timeout = TimeSpan.FromSeconds(1)
        };

        var response = await agent.Serf!.QueryAsync("test-query", payload, queryParam);

        Assert.NotNull(response);

        await agent.DisposeAsync();
    }

    [Fact]
    public async Task Agent_ForceLeave_RemovesFailedNode()
    {
        var config = new AgentConfig
        {
            NodeName = "test-force-leave",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        // Try to force leave non-existent node (should not throw)
        await agent.Serf!.RemoveFailedNodeAsync("nonexistent-node");

        await agent.DisposeAsync();
    }

    [Fact]
    public async Task Agent_ForceLeavePrune_PrunesCompletely()
    {
        var config = new AgentConfig
        {
            NodeName = "test-prune",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        // Try to prune non-existent node (should not throw)
        await agent.Serf!.RemoveFailedNodeAsync("nonexistent-node", prune: true);

        await agent.DisposeAsync();
    }

    [Fact]
    public async Task Agent_UpdateTags_ModifiesTags()
    {
        var config = new AgentConfig
        {
            NodeName = "test-update-tags",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        var newTags = new Dictionary<string, string>
        {
            ["env"] = "production",
            ["version"] = "2.0"
        };

        await agent.SetTagsAsync(newTags);

        var localMember = agent.Serf!.LocalMember();
        Assert.Equal("production", localMember.Tags["env"]);
        Assert.Equal("2.0", localMember.Tags["version"]);

        await agent.DisposeAsync();
    }

    [Fact]
    public async Task Agent_Members_ReturnsClusterMembers()
    {
        var config = new AgentConfig
        {
            NodeName = "test-members",
            BindAddr = "127.0.0.1:0"
        };

        var agent = new SerfAgent(config);
        await agent.StartAsync();

        var members = agent.Serf!.Members();

        Assert.Single(members);  // Only local node
        Assert.Equal("test-members", members[0].Name);

        await agent.DisposeAsync();
    }
}
