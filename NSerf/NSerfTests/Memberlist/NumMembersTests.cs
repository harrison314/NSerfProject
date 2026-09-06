// Copyright (c) Boolhak, Inc.
// SPDX-License-Identifier: MPL-2.0
// Ported from: github.com/hashicorp/memberlist/memberlist.go (NumMembers / estNumNodes)

using System.Net;
using FluentAssertions;
using NSerf.Memberlist;
using NSerf.Memberlist.Configuration;
using NSerf.Memberlist.Messages;
using NSerf.Memberlist.State;
using NSerfTests.Memberlist.Transport;
using Xunit;

namespace NSerfTests.Memberlist;

/// <summary>
/// Go's Memberlist.NumMembers() counts only nodes that are not dead or left,
/// while estNumNodes() (the retransmit / suspicion scaling estimate) keeps counting
/// every node that is still in the node map.
/// </summary>
public class NumMembersTests
{
    private static MemberlistConfig CreateTestConfig(string name)
    {
        var config = MemberlistConfig.DefaultLANConfig();
        config.Name = name;
        config.BindPort = 0;
        config.Logger = null;
        return config;
    }

    private static Alive MakeAlive(MemberlistConfig config, string name, int lastOctet) => new()
    {
        Node = name,
        Addr = IPAddress.Parse($"10.0.0.{lastOctet}").GetAddressBytes(),
        Port = 7946,
        Incarnation = 1,
        Meta = [],
        Vsn =
        [
            ProtocolVersion.Min, ProtocolVersion.Max, config.ProtocolVersion,
            config.DelegateProtocolMin, config.DelegateProtocolMax, config.DelegateProtocolVersion
        ]
    };

    [Fact]
    public async Task NumMembers_ExcludesDeadAndLeftNodes_WhileEstNumNodesIsUnchanged()
    {
        var network = new MockNetwork();
        var config = CreateTestConfig("node1");
        config.Transport = network.CreateTransport("node1");

        var m = NSerf.Memberlist.Memberlist.Create(config);
        try
        {
            var stateHandler = new StateHandlers(m, config.Logger);
            stateHandler.HandleAliveNode(MakeAlive(config, "node2", 2), false, null);
            stateHandler.HandleAliveNode(MakeAlive(config, "node3", 3), false, null);
            stateHandler.HandleAliveNode(MakeAlive(config, "node4", 4), false, null);

            m.NumMembers().Should().Be(4, "self + three alive peers");
            m.EstNumNodes().Should().Be(4);

            // node2 fails (reported by another node) -> Dead
            stateHandler.HandleDeadNode(new Dead { Node = "node2", From = "node1", Incarnation = 1 });
            // node3 leaves gracefully (Node == From) -> Left
            stateHandler.HandleDeadNode(new Dead { Node = "node3", From = "node3", Incarnation = 1 });

            m.NodeMap["node2"].State.Should().Be(NodeStateType.Dead);
            m.NodeMap["node3"].State.Should().Be(NodeStateType.Left);
            m.NodeMap.Count.Should().Be(4, "dead/left nodes stay in the node map until reaped");

            m.NumMembers().Should().Be(2, "Go's NumMembers counts only nodes that are not dead or left (self + node4)");
            m.EstNumNodes().Should().Be(4, "the node estimate must not change when nodes die or leave");
        }
        finally
        {
            await m.ShutdownAsync();
        }
    }

    [Fact]
    public async Task NumMembers_CountsSuspectNodes()
    {
        var network = new MockNetwork();
        var config = CreateTestConfig("node1");
        config.Transport = network.CreateTransport("node1");

        var m = NSerf.Memberlist.Memberlist.Create(config);
        try
        {
            var stateHandler = new StateHandlers(m, config.Logger);
            stateHandler.HandleAliveNode(MakeAlive(config, "node2", 2), false, null);

            lock (m.NodeLock)
            {
                m.NodeMap["node2"].State = NodeStateType.Suspect;
                m.NodeMap["node2"].Node.State = NodeStateType.Suspect;
            }

            m.NumMembers().Should().Be(2, "suspect nodes are neither dead nor left");
            m.EstNumNodes().Should().Be(2);
        }
        finally
        {
            await m.ShutdownAsync();
        }
    }
}
