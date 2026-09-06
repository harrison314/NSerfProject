// Ported from: github.com/hashicorp/memberlist/state_test.go (probeNode indirect / awareness / nack tests)
// Copyright (c) Boolhak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Diagnostics;
using System.Net;
using NSerf.Memberlist;
using NSerf.Memberlist.Configuration;
using NSerf.Memberlist.Handlers;
using NSerf.Memberlist.Messages;
using NSerf.Memberlist.State;
using NSerf.Memberlist.Transport;
using NSerfTests.Memberlist.Transport;

namespace NSerfTests.Memberlist;

/// <summary>
/// Specifies the SWIM indirect-ping path of probeNode: indirect ping requests, nack handling,
/// awareness scoring and the intermediary's nack timer (Go memberlist state.go / net.go).
/// </summary>
public class IndirectPingTests : IAsyncLifetime
{
    private readonly List<NSerf.Memberlist.Memberlist> _memberlists = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var m in _memberlists)
        {
            try { await m.ShutdownAsync(); } catch { /* ignore */ }
        }
    }

    private static MemberlistConfig CreateConfig(string name, MockNetwork network, TimeSpan probeTimeout, TimeSpan probeInterval)
    {
        var config = MemberlistConfig.DefaultLANConfig();
        config.Name = name;
        config.BindPort = 0;
        config.Logger = null;
        config.ProbeTimeout = probeTimeout;
        config.ProbeInterval = probeInterval;
        config.GossipInterval = TimeSpan.FromSeconds(10); // keep gossip out of the way
        config.PushPullInterval = TimeSpan.Zero;
        config.Transport = network.CreateTransport(name);
        return config;
    }

    private NSerf.Memberlist.Memberlist Create(MemberlistConfig config)
    {
        var m = NSerf.Memberlist.Memberlist.Create(config);
        _memberlists.Add(m);
        return m;
    }

    private static byte[] Vsn(byte pMax) =>
        [ProtocolVersion.Min, pMax, ProtocolVersion.Version2Compatible, 1, 1, 1];

    private static NodeState AddAliveNode(NSerf.Memberlist.Memberlist m, string name, IPAddress addr, int port, byte pMax = ProtocolVersion.Max)
    {
        var alive = new Alive
        {
            Node = name,
            Addr = addr.GetAddressBytes(),
            Port = (ushort)port,
            Incarnation = 1,
            Meta = [],
            Vsn = Vsn(pMax)
        };
        new StateHandlers(m, null).HandleAliveNode(alive, false, null);
        m.NodeMap.TryGetValue(name, out var state).Should().BeTrue();
        return state!;
    }

    private static NodeState AddAliveNode(NSerf.Memberlist.Memberlist m, NSerf.Memberlist.Memberlist other, byte pMax = ProtocolVersion.Max)
    {
        var (addr, port) = other.GetAdvertiseAddr();
        return AddAliveNode(m, other.Config.Name, addr, port, pMax);
    }

    private static NodeState AddAliveNode(NSerf.Memberlist.Memberlist m, string name, MockTransport transport, byte pMax = ProtocolVersion.Max)
    {
        var (addr, port) = transport.FinalAdvertiseAddr("", 0);
        return AddAliveNode(m, name, addr, port, pMax);
    }

    private static Address AddressOf(NSerf.Memberlist.Memberlist m)
    {
        var (addr, port) = m.GetAdvertiseAddr();
        return new Address { Addr = $"{addr}:{port}", Name = m.Config.Name };
    }

    private static List<byte[]> DrainPackets(MockTransport transport)
    {
        var packets = new List<byte[]>();
        while (transport.PacketChannel.TryRead(out var packet))
        {
            packets.Add(packet.Buf);
        }
        return packets;
    }

    /// <summary>
    /// (a) The direct UDP path to the target is broken, TCP fallback is disabled, but other nodes can reach
    /// the target: the prober must obtain an indirect ack and keep the target Alive.
    /// Three intermediaries are used because kRandomNodes is randomised (each eligible node has a ~5%
    /// chance of not being picked in a round); with three the chance that none relays is negligible.
    /// </summary>
    [Fact]
    public async Task Probe_DirectUdpDropped_IndirectAckKeepsTargetAlive()
    {
        var network = new MockNetwork();
        var config1 = CreateConfig("node1", network, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));
        config1.DisableTcpPings = true;
        var m1 = Create(config1);
        var m2 = Create(CreateConfig("node2", network, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5)));

        // node1 cannot reach node2 over UDP, everything else flows
        ((MockTransport)config1.Transport!).ShouldDropPacketTo = dest => dest == "node2";

        var node2 = AddAliveNode(m1, m2);
        foreach (var name in new[] { "node3", "node4", "node5" })
        {
            var intermediary = Create(CreateConfig(name, network, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5)));
            AddAliveNode(m1, intermediary);
        }

        await m1.ProbeNodeAsync(node2);

        m1.NodeMap["node2"].State.Should().Be(NodeStateType.Alive,
            "an indirect ack relayed by an intermediary must count as a successful probe");
        m1.GetHealthScore().Should().Be(0, "a successful probe must not degrade the local health score");

        // Let the background prober run a few more rounds through the same broken direct path
        await Task.Delay(1200);
        m1.NodeMap["node2"].State.Should().Be(NodeStateType.Alive, "node2 must stay alive across probe rounds");
        m1.GetHealthScore().Should().Be(0);
    }

    /// <summary>
    /// (b) After the direct ping times out, IndirectPing requests go to min(IndirectChecks, eligible peers)
    /// alive peers (never self, the target, or non-alive nodes) carrying the probe seq, target and nack flag.
    /// </summary>
    [Fact]
    public async Task Probe_DirectUdpTimeout_SendsIndirectPingsToEligiblePeers()
    {
        var network = new MockNetwork();
        var config1 = CreateConfig("node1", network, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(600));
        config1.DisableTcpPings = true;
        config1.IndirectChecks = 2;
        var m1 = Create(config1);

        // Target: nobody listens here, so the direct ping is lost
        var targetAddr = IPAddress.Loopback;
        const int targetPort = 9999;
        var target = AddAliveNode(m1, "node2", targetAddr, targetPort);

        // Six alive raw peers (three nack-capable, three too old for nacks), one dead and one suspect node
        var peers = new Dictionary<string, (MockTransport Transport, byte PMax)>();
        for (var i = 0; i < 6; i++)
        {
            var name = $"peer{i}";
            var pMax = i < 3 ? ProtocolVersion.Max : (byte)3;
            var transport = network.CreateTransport(name);
            AddAliveNode(m1, name, transport, pMax);
            peers[name] = (transport, pMax);
        }

        var deadTransport = network.CreateTransport("dead-peer");
        var deadState = AddAliveNode(m1, "dead-peer", deadTransport);
        var suspectTransport = network.CreateTransport("suspect-peer");
        var suspectState = AddAliveNode(m1, "suspect-peer", suspectTransport);
        lock (m1.NodeLock)
        {
            deadState.State = NodeStateType.Dead;
            suspectState.State = NodeStateType.Suspect;
        }

        var seqBefore = m1.SequenceNum;
        var (selfAddr, selfPort) = m1.GetAdvertiseAddr();

        await m1.ProbeNodeAsync(target);

        var requests = new List<(string Peer, IndirectPingMessage Msg)>();
        foreach (var (name, (transport, _)) in peers)
        {
            foreach (var buf in DrainPackets(transport))
            {
                if (buf[0] != (byte)MessageType.IndirectPing) continue;
                requests.Add((name, MessageEncoder.Decode<IndirectPingMessage>(buf.AsSpan(1))));
            }
        }

        requests.Should().HaveCount(2, "IndirectChecks=2 with six eligible peers must yield exactly two requests");
        requests.Select(r => r.Peer).Should().OnlyHaveUniqueItems("each peer is asked at most once per probe");
        DrainPackets(deadTransport).Should().NotContain(b => b[0] == (byte)MessageType.IndirectPing, "dead nodes are not eligible");
        DrainPackets(suspectTransport).Should().NotContain(b => b[0] == (byte)MessageType.IndirectPing, "suspect nodes are not eligible");

        foreach (var (peer, msg) in requests)
        {
            msg.SeqNo.Should().Be(seqBefore + 1, "indirect requests reuse the direct ping's sequence number");
            msg.Target.Should().Equal(targetAddr.GetAddressBytes());
            msg.Port.Should().Be(targetPort);
            msg.Node.Should().Be("node2");
            msg.SourceAddr.Should().Equal(selfAddr.GetAddressBytes());
            msg.SourcePort.Should().Be((ushort)selfPort);
            msg.SourceNode.Should().Be("node1");
            msg.Nack.Should().Be(peers[peer].PMax >= 4, "nacks are only requested from peers speaking protocol >= 4");
        }

        m1.NodeMap["node2"].State.Should().Be(NodeStateType.Suspect, "no ack at all must still mark the target suspect");
    }

    /// <summary>
    /// (c) A MessagePack-encoded NackRespMessage arriving over the packet path must reach the nack handler
    /// registered for its sequence number.
    /// </summary>
    [Fact]
    public async Task HandleNack_MessagePackEncodedNack_InvokesRegisteredNackHandler()
    {
        var network = new MockNetwork();
        var m1 = Create(CreateConfig("node1", network, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5)));
        var sender = network.CreateTransport("sender");

        const uint seqNo = 777;
        var nacked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new AckNackHandler(null);
        handler.SetAckHandler(seqNo, (_, _) => { }, () => nacked.TrySetResult(true), TimeSpan.FromSeconds(10));
        m1.AckHandlers[seqNo] = handler;

        var nack = MessageEncoder.Encode(MessageType.NackResp, new NackRespMessage { SeqNo = seqNo });
        await sender.WriteToAddressAsync(nack, AddressOf(m1));

        var completed = await Task.WhenAny(nacked.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        completed.Should().BeSameAs(nacked.Task, "the nack handler for seq 777 must be invoked from a MessagePack-encoded nack");
    }

    /// <summary>
    /// A nack must not tear down the ack handler: a later ack for the same probe (from another intermediary)
    /// still has to be delivered (Go's invokeNackHandler leaves the handler registered).
    /// </summary>
    [Fact]
    public void AckNackHandler_NackThenAck_BothCallbacksFire()
    {
        var handler = new AckNackHandler(null);
        var nacks = 0;
        var acked = false;
        const uint seqNo = 31;
        handler.SetAckHandler(seqNo, (_, _) => acked = true, () => nacks++, TimeSpan.FromSeconds(10));

        handler.InvokeNack(seqNo);
        handler.InvokeNack(seqNo);
        handler.InvokeAck(seqNo, [], DateTimeOffset.UtcNow);

        nacks.Should().Be(2, "every nack is counted");
        acked.Should().BeTrue("an ack after nacks must still complete the probe");
        handler.PendingCount.Should().Be(0, "the ack removes the handler");
    }

    /// <summary>
    /// (d) A successful direct probe improves the health score by one.
    /// </summary>
    [Fact]
    public async Task Probe_Success_DecreasesAwarenessScore()
    {
        var network = new MockNetwork();
        var m1 = Create(CreateConfig("node1", network, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(2)));
        var m2 = Create(CreateConfig("node2", network, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)));
        var node2 = AddAliveNode(m1, m2);

        m1.Awareness.ApplyDelta(2);
        m1.GetHealthScore().Should().Be(2);

        await m1.ProbeNodeAsync(node2);

        m1.GetHealthScore().Should().Be(1, "a successful probe applies an awareness delta of -1");
        m1.NodeMap["node2"].State.Should().Be(NodeStateType.Alive);
    }

    /// <summary>
    /// (d) A failed probe with no nack-capable peers degrades the health score by one, and the prober waits
    /// the full probe interval for indirect acks before suspecting the node.
    /// </summary>
    [Fact]
    public async Task Probe_FailureWithoutNackCapablePeers_IncreasesAwarenessByOne()
    {
        var network = new MockNetwork();
        var probeInterval = TimeSpan.FromMilliseconds(500);
        var config1 = CreateConfig("node1", network, TimeSpan.FromMilliseconds(50), probeInterval);
        config1.DisableTcpPings = true;
        var m1 = Create(config1);
        var ghost = AddAliveNode(m1, "ghost", IPAddress.Loopback, 9998);

        m1.GetHealthScore().Should().Be(0);

        var sw = Stopwatch.StartNew();
        await m1.ProbeNodeAsync(ghost);
        sw.Stop();

        m1.GetHealthScore().Should().Be(1, "a failed probe without nack-capable peers applies an awareness delta of +1");
        m1.NodeMap["ghost"].State.Should().Be(NodeStateType.Suspect);
        sw.Elapsed.Should().BeGreaterThanOrEqualTo(probeInterval - TimeSpan.FromMilliseconds(100),
            "the prober waits for indirect acks until the probe interval elapses");
    }

    /// <summary>
    /// (e) An intermediary that cannot reach the target answers a nack-requesting IndirectPing after its
    /// configured ProbeTimeout (not a hard-coded 5 seconds).
    /// </summary>
    [Fact]
    public async Task HandleIndirectPing_TargetUnreachable_SendsNackAfterProbeTimeout()
    {
        var network = new MockNetwork();
        var intermediary = Create(CreateConfig("node3", network, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5)));
        var requester = network.CreateTransport("requester");
        var (reqAddr, reqPort) = requester.FinalAdvertiseAddr("", 0);

        const uint seqNo = 4242;
        var request = new IndirectPingMessage
        {
            SeqNo = seqNo,
            Target = IPAddress.Loopback.GetAddressBytes(),
            Port = 9997, // nobody listens here
            Node = "ghost",
            Nack = true,
            SourceAddr = reqAddr.GetAddressBytes(),
            SourcePort = (ushort)reqPort,
            SourceNode = "requester"
        };

        var sw = Stopwatch.StartNew();
        await requester.WriteToAddressAsync(MessageEncoder.Encode(MessageType.IndirectPing, request), AddressOf(intermediary));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        Packet? packet = null;
        try
        {
            packet = await requester.PacketChannel.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // handled by the assertion below
        }
        sw.Stop();

        packet.Should().NotBeNull("the intermediary must send a nack within 2s when ProbeTimeout is 100ms");
        packet!.Buf[0].Should().Be((byte)MessageType.NackResp);
        MessageEncoder.Decode<NackRespMessage>(packet.Buf.AsSpan(1)).SeqNo.Should().Be(seqNo,
            "the nack carries the requester's original sequence number");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "the nack timer is ProbeTimeout, not 5 seconds");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// The intermediary registers a temporary ack handler for every forwarded indirect ping. When the
    /// target answers, the ack is relayed and the entry must be removed again (Go: invokeAckHandler
    /// deletes the handler), otherwise every relayed probe leaks an entry for the life of the process.
    /// </summary>
    [Fact]
    public async Task HandleIndirectPing_TargetReachable_ForwardsAckAndRemovesAckHandler()
    {
        var network = new MockNetwork();
        var intermediary = Create(CreateConfig("node3", network, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5)));
        var target = Create(CreateConfig("target", network, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5)));
        var requester = network.CreateTransport("requester");
        var (reqAddr, reqPort) = requester.FinalAdvertiseAddr("", 0);
        var (targetAddr, targetPort) = target.GetAdvertiseAddr();

        intermediary.AckHandlers.Should().BeEmpty();

        const uint seqNo = 5151;
        var request = new IndirectPingMessage
        {
            SeqNo = seqNo,
            Target = targetAddr.GetAddressBytes(),
            Port = (ushort)targetPort,
            Node = "target",
            Nack = true,
            SourceAddr = reqAddr.GetAddressBytes(),
            SourcePort = (ushort)reqPort,
            SourceNode = "requester"
        };

        await requester.WriteToAddressAsync(MessageEncoder.Encode(MessageType.IndirectPing, request), AddressOf(intermediary));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var packet = await requester.PacketChannel.ReadAsync(cts.Token);

        packet.Buf[0].Should().Be((byte)MessageType.AckResp, "the intermediary relays the target's ack");
        MessageEncoder.Decode<AckRespMessage>(packet.Buf.AsSpan(1)).SeqNo.Should().Be(seqNo,
            "the relayed ack carries the requester's original sequence number");

        await WaitUntilAsync(() => intermediary.AckHandlers.IsEmpty, TimeSpan.FromSeconds(1));
        intermediary.AckHandlers.Should().BeEmpty(
            "the temporary handler registered for the forwarded ping must be removed once the ack is relayed");
    }

    /// <summary>
    /// Same as above for the timeout path: when the target never answers, the nack is sent and the
    /// temporary handler entry must be removed (Go: setAckHandler's timer deletes the handler).
    /// </summary>
    [Fact]
    public async Task HandleIndirectPing_TargetUnreachable_RemovesAckHandlerAfterNackTimeout()
    {
        var network = new MockNetwork();
        var intermediary = Create(CreateConfig("node3", network, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5)));
        var requester = network.CreateTransport("requester");
        var (reqAddr, reqPort) = requester.FinalAdvertiseAddr("", 0);

        intermediary.AckHandlers.Should().BeEmpty();

        var request = new IndirectPingMessage
        {
            SeqNo = 6161,
            Target = IPAddress.Loopback.GetAddressBytes(),
            Port = 9997, // nobody listens here
            Node = "ghost",
            Nack = true,
            SourceAddr = reqAddr.GetAddressBytes(),
            SourcePort = (ushort)reqPort,
            SourceNode = "requester"
        };

        await requester.WriteToAddressAsync(MessageEncoder.Encode(MessageType.IndirectPing, request), AddressOf(intermediary));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var packet = await requester.PacketChannel.ReadAsync(cts.Token);
        packet.Buf[0].Should().Be((byte)MessageType.NackResp, "the intermediary nacks after ProbeTimeout");

        await WaitUntilAsync(() => intermediary.AckHandlers.IsEmpty, TimeSpan.FromSeconds(1));
        intermediary.AckHandlers.Should().BeEmpty(
            "the temporary handler registered for the forwarded ping must be removed when its timer fires");
    }
}
