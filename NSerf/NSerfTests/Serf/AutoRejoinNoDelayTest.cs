// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0
// Reference: github.com/hashicorp/serf/serf/serf.go Create() -> handleRejoin (no fixed sleep)

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using NSerf.Memberlist.Configuration;
using NSerf.Serf;
using Xunit;

namespace NSerfTests.Serf;

/// <summary>
/// Go's Serf.Create starts the snapshot auto-rejoin without any fixed sleep; memberlist's
/// listeners are already running when Create returns. CreateAsync must therefore return
/// promptly even when every previous node in the snapshot is unreachable.
/// </summary>
[Collection("Sequential Snapshot Tests")]
public class AutoRejoinNoDelayTest : IDisposable
{
    private readonly string _snapshotPath = Path.Combine(Path.GetTempPath(), $"serf_rejoin_{Guid.NewGuid()}.snapshot");

    public void Dispose()
    {
        try
        {
            if (File.Exists(_snapshotPath)) File.Delete(_snapshotPath);
        }
        catch
        {
            // ignore cleanup errors
        }
    }

    private static int GetClosedLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    [Fact]
    public async Task CreateAsync_WithUnreachablePreviousNode_DoesNotSleepBeforeReturning()
    {
        var closedPort = GetClosedLoopbackPort();
        await File.WriteAllTextAsync(_snapshotPath, $"alive: ghost 127.0.0.1:{closedPort}\nclock: 5\n");

        var config = new Config
        {
            NodeName = "rejoiner",
            SnapshotPath = _snapshotPath,
            MemberlistConfig = new MemberlistConfig
            {
                Name = "rejoiner",
                BindAddr = "127.0.0.1",
                BindPort = 0,
                TCPTimeout = TimeSpan.FromMilliseconds(200)
            }
        };

        var sw = Stopwatch.StartNew();
        using var serf = await NSerf.Serf.Serf.CreateAsync(config);
        sw.Stop();

        try
        {
            serf.NumMembers().Should().Be(1, "the only previous node is unreachable");
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(900),
                "CreateAsync must not insert a fixed delay before the best-effort rejoin attempt");
        }
        finally
        {
            await serf.ShutdownAsync();
        }
    }
}
