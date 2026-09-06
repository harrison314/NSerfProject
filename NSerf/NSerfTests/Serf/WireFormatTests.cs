// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Text;
using MessagePack;
using NSerf.Client;
using NSerf.Memberlist.Common;
using NSerf.Memberlist.Messages;
using NSerf.Serf;
using MemberlistMessageType = NSerf.Memberlist.Messages.MessageType;
using SerfMessageType = NSerf.Serf.MessageType;

namespace NSerfTests.Serf;

/// <summary>
/// Characterisation tests that pin NSerf's actual on-wire encoding so it cannot drift by accident.
///
/// NSerf is NOT wire compatible with Go serf/memberlist. These tests document the differences:
///   - every message is a MessagePack ARRAY (integer <c>[Key(n)]</c>) in Go's field order,
///     whereas Go encodes structs as MAPS keyed by field name;
///   - <see cref="LamportTime"/> is itself a <c>[MessagePackObject]</c> and therefore nests as a
///     one-element array, not a bare integer;
///   - <see cref="MessageQuery.Timeout"/> is a <see cref="TimeSpan"/> serialised as 100 ns ticks,
///     where Go sends <c>time.Duration</c> nanoseconds;
///   - gossip compression is GZip, where Go uses LZW;
///   - the RPC protocol shares the same array encoding, so only the NSerf client/CLI can talk to an
///     NSerf agent.
///
/// These tests pass by design against the current implementation; they are documentation of
/// behaviour, not a red/green cycle. If one of them fails, the wire format has changed and the
/// README's wire-format section must be updated together with the code.
/// </summary>
public class WireFormatTests
{
    // MessagePack format bytes (https://github.com/msgpack/msgpack/blob/master/spec.md)
    private const byte FixArray0 = 0x90;
    private const byte FixStr0 = 0xA0;
    private const byte False = 0xC2;
    private const byte True = 0xC3;
    private const byte Bin8 = 0xC4;
    private const byte UInt16 = 0xCD;

    private static byte FixArray(int n) => (byte)(FixArray0 | n);
    private static byte FixStr(int n) => (byte)(FixStr0 | n);

    private static byte[] Concat(params byte[][] parts)
    {
        using var ms = new MemoryStream();
        foreach (var p in parts) ms.Write(p);
        return ms.ToArray();
    }

    private static byte[] Str(string s) => Concat([FixStr(Encoding.UTF8.GetByteCount(s))], Encoding.UTF8.GetBytes(s));
    private static byte[] Bin(byte[] b) => Concat([Bin8, (byte)b.Length], b);

    /// <summary>LamportTime is a [MessagePackObject] struct with a single [Key(0)] => [value].</summary>
    private static byte[] LTime(byte fixint) => [FixArray(1), fixint];

    [Fact]
    public void LamportTime_EncodesAsSingleElementArray_NotBareInteger()
    {
        var bytes = MessagePackSerializer.Serialize(new LamportTime(7));

        bytes.Should().Equal(LTime(0x07),
            "LamportTime is a [MessagePackObject] and therefore nests as a one-element array; Go sends a bare uint64");
        MessagePackSerializer.Deserialize<LamportTime>(bytes).Should().Be(new LamportTime(7));
    }

    [Fact]
    public void MessageJoin_EncodesAsArray_LTime_Node()
    {
        var msg = new MessageJoin { LTime = 5, Node = "n1" };

        var bytes = MessagePackSerializer.Serialize(msg);

        bytes.Should().Equal(Concat([FixArray(2)], LTime(0x05), Str("n1")));
        (bytes[0] & 0xF0).Should().Be(FixArray0, "NSerf uses array encoding, not Go's map-by-field-name encoding");
    }

    [Fact]
    public void MessageLeave_EncodesAsArray_LTime_Node_Prune()
    {
        var msg = new MessageLeave { LTime = 9, Node = "n2", Prune = true };

        var bytes = MessagePackSerializer.Serialize(msg);

        bytes.Should().Equal(Concat([FixArray(3)], LTime(0x09), Str("n2"), [True]));
    }

    [Fact]
    public void MessageUserEvent_EncodesAsArray_LTime_Name_Payload_CC()
    {
        var msg = new MessageUserEvent { LTime = 3, Name = "ev", Payload = [0x01, 0x02], CC = false };

        var bytes = MessagePackSerializer.Serialize(msg);

        bytes.Should().Equal(Concat([FixArray(4)], LTime(0x03), Str("ev"), Bin([0x01, 0x02]), [False]));
    }

    [Fact]
    public void MessageCodec_EncodeMessage_PrefixesSerfMessageTypeByte()
    {
        var msg = new MessageUserEvent { LTime = 3, Name = "ev", Payload = [0x01, 0x02] };

        var framed = MessageCodec.EncodeMessage(SerfMessageType.UserEvent, msg);

        framed[0].Should().Be((byte)SerfMessageType.UserEvent);
        framed[0].Should().Be(3, "Serf message type values match Go: leave=0, join=1, pushPull=2, userEvent=3, query=4 ...");
        framed.Skip(1).Should().Equal(MessagePackSerializer.Serialize(msg));

        var decoded = MessageCodec.DecodeMessage<MessageUserEvent>(framed.Skip(1).ToArray());
        decoded.Name.Should().Be("ev");
        decoded.LTime.Should().Be(new LamportTime(3));
    }

    [Fact]
    public void MessageQuery_Timeout_RoundTripsAsTimeSpanTicks()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var msg = new MessageQuery
        {
            LTime = 1,
            ID = 42,
            Addr = Encoding.UTF8.GetBytes("127.0.0.1"), // NSerf sends the IP as UTF-8 text; Go sends raw IP bytes
            Port = 7946,
            SourceNode = "src",
            Filters = [],
            Flags = (uint)QueryFlags.Ack,
            RelayFactor = 2,
            Timeout = timeout,
            Name = "q",
            Payload = [],
        };

        var bytes = MessagePackSerializer.Serialize(msg);

        // Walk the array to element 8 (Timeout) and confirm it is a raw Int64 of .NET ticks, not Go nanoseconds.
        var reader = new MessagePackReader(bytes);
        reader.ReadArrayHeader().Should().Be(11, "MessageQuery has 11 keyed members in Go's field order");
        for (var i = 0; i < 8; i++) reader.Skip();
        reader.ReadInt64().Should().Be(timeout.Ticks, "TimeSpan is serialised as 100 ns ticks");
        timeout.Ticks.Should().Be(50_000_000L);
        timeout.Ticks.Should().NotBe(5_000_000_000L, "Go would send time.Duration nanoseconds");

        var decoded = MessagePackSerializer.Deserialize<MessageQuery>(bytes);
        decoded.Timeout.Should().Be(timeout);
        decoded.ID.Should().Be(42u);
        decoded.Port.Should().Be(7946);
        Encoding.UTF8.GetString(decoded.Addr).Should().Be("127.0.0.1");
        decoded.IsAck.Should().BeTrue();
    }

    [Fact]
    public void AliveMessage_EncodesAsArray_Incarnation_Node_Addr_Port_Meta_Vsn_WithTypePrefix()
    {
        var msg = new AliveMessage
        {
            Incarnation = 1,
            Node = "a",
            Addr = [127, 0, 0, 1],
            Port = 7946,
            Meta = [],
            Vsn = [1, 5, 2, 1, 4, 4],
        };

        var body = MessagePackSerializer.Serialize(msg);
        body.Should().Equal(Concat(
            [FixArray(6)],
            [0x01],
            Str("a"),
            Bin([127, 0, 0, 1]),
            [UInt16, 0x1F, 0x0A],
            Bin([]),
            Bin([1, 5, 2, 1, 4, 4])));

        var framed = MessageEncoder.Encode(MemberlistMessageType.Alive, msg);
        framed[0].Should().Be((byte)MemberlistMessageType.Alive);
        framed[0].Should().Be(4, "memberlist message type values match Go: ping=0 ... alive=4, dead=5, pushPull=6");
        framed.Skip(1).Should().Equal(body);

        var decoded = MessageEncoder.Decode<AliveMessage>(framed.AsSpan(1));
        decoded.Node.Should().Be("a");
        decoded.Port.Should().Be(7946);
        decoded.Vsn.Should().Equal(1, 5, 2, 1, 4, 4);
    }

    [Fact]
    public void DeadMessage_EncodesAsArray_Incarnation_Node_From_WithTypePrefix()
    {
        var msg = new DeadMessage { Incarnation = 2, Node = "b", From = "a" };

        var framed = MessageEncoder.Encode(MemberlistMessageType.Dead, msg);

        framed.Should().Equal(Concat([(byte)MemberlistMessageType.Dead, FixArray(3), 0x02], Str("b"), Str("a")));
        framed[0].Should().Be(5);
    }

    [Fact]
    public void RpcRequestHeader_EncodesAsArray_Command_Seq()
    {
        var header = new RequestHeader { Command = "handshake", Seq = 1 };

        var bytes = MessagePackSerializer.Serialize(header);

        bytes.Should().Equal(Concat([FixArray(2)], Str("handshake"), [0x01]),
            "RPC headers are array encoded too, so the Go serf CLI cannot talk to an NSerf agent");
    }

    [Fact]
    public void CompressionUtils_UsesGZip_NotLzw()
    {
        var payload = Encoding.UTF8.GetBytes(new string('x', 512));

        var compressed = CompressionUtils.CompressPayload(payload);

        // 0x1F 0x8B is the GZip member header magic; Go memberlist uses LZW.
        compressed.Take(2).Should().Equal(0x1F, 0x8B);
        CompressionUtils.DecompressPayload(compressed).Should().Equal(payload);
    }
}
