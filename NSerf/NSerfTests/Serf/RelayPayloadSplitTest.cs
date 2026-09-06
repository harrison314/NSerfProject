// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0
// Reference: github.com/hashicorp/serf/serf/delegate.go NotifyMsg (messageRelayType)

using System.Net;
using System.Text;
using FluentAssertions;
using MessagePack;
using NSerf.Serf;
using Xunit;
using SerfDelegate = NSerf.Serf.Delegate;

namespace NSerfTests.Serf;

/// <summary>
/// The relay handler must find the inner message by the number of bytes the decoder actually
/// consumed for the relay header (Go: reads from the same buffer), not by re-encoding the header.
/// </summary>
public class RelayPayloadSplitTest
{
    private static MessageQueryResponse SampleResponse() => new()
    {
        LTime = 42,
        ID = 1234,
        From = "responder",
        Payload = Encoding.UTF8.GetBytes("pong")
    };

    [Fact]
    public void SplitRelayPayload_CanonicalEncoding_ReturnsHeaderAndInnerMessage()
    {
        var resp = SampleResponse();
        var dest = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 8080);
        var encoded = MessageCodec.EncodeRelayMessage(MessageType.QueryResponse, dest, "dest-node", resp);
        encoded[0].Should().Be((byte)MessageType.Relay);

        var (header, inner) = SerfDelegate.SplitRelayPayload(encoded[1..]);

        header.DestName.Should().Be("dest-node");
        header.DestAddr.IP.Should().Equal(127, 0, 0, 1);
        header.DestAddr.Port.Should().Be(8080);

        inner[0].Should().Be((byte)MessageType.QueryResponse);
        var decoded = MessagePackSerializer.Deserialize<MessageQueryResponse>(inner[1..]);
        decoded.ID.Should().Be(resp.ID);
        decoded.From.Should().Be(resp.From);
        decoded.Payload.Should().Equal(resp.Payload);

        var expectedInner = new byte[] { (byte)MessageType.QueryResponse }
            .Concat(MessagePackSerializer.Serialize(resp)).ToArray();
        inner.Should().Equal(expectedInner, "the inner message must be forwarded byte-for-byte");
    }

    [Fact]
    public void SplitRelayPayload_NonMinimalIntegerWidthInHeader_StillFindsInnerMessage()
    {
        // Hand-built header: fixarray(2) [ fixarray(3) [ bin8 ip, uint32 port, nil zone ], str name ]
        // The port 8080 is written with a 32-bit width (0xce) although it fits in 16 bits: a
        // non-canonical but valid MessagePack encoding that a re-serialisation would shrink.
        var headerBytes = new byte[]
        {
            0x92,                                   // fixarray(2)
            0x93,                                   // fixarray(3)  -> UdpAddr
            0xc4, 0x04, 127, 0, 0, 1,               // bin8 IP
            0xce, 0x00, 0x00, 0x1f, 0x90,           // uint32 8080
            0xc0,                                   // nil zone
            0xa4, (byte)'d', (byte)'e', (byte)'s', (byte)'t' // fixstr "dest"
        };

        var resp = SampleResponse();
        var innerBytes = new byte[] { (byte)MessageType.QueryResponse }
            .Concat(MessagePackSerializer.Serialize(resp)).ToArray();
        var payload = headerBytes.Concat(innerBytes).ToArray();

        var (header, inner) = SerfDelegate.SplitRelayPayload(payload);

        header.DestName.Should().Be("dest");
        header.DestAddr.Port.Should().Be(8080);
        header.DestAddr.IP.Should().Equal(127, 0, 0, 1);
        inner.Should().Equal(innerBytes, "the split must follow the bytes actually consumed by the header decoder");
        inner[0].Should().Be((byte)MessageType.QueryResponse);
        MessagePackSerializer.Deserialize<MessageQueryResponse>(inner[1..]).From.Should().Be("responder");
    }
}
