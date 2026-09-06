// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NSerf.Serf;
using NSerf.Serf.Events;
using NSerf.Memberlist.Configuration;
using NSerf.Metrics;

namespace NSerfTests.Serf;

/// <summary>
/// Reusable test helper utilities for Serf testing.
/// Provides common test configuration, waiting helpers, and validation methods.
/// </summary>
public static class TestHelpers
{
    private static readonly object _portLock = new();
    private static int _portCounter = 10000;
    private static int _ipCounter;

    /// <summary>
    /// Creates a test Serf configuration with aggressive timeouts for faster tests.
    /// Each call gets a unique port number for parallel test execution.
    /// </summary>
    /// <param name="nodeName">Optional node name. If not provided, generates unique name.</param>
    /// <returns>Configured SerfConfig for testing</returns>
    public static SerfConfig CreateTestConfig(string? nodeName = null)
    {
        var port = GetFreePort();

        nodeName ??= $"test-node-{port}";

        return new SerfConfig
        {
            NodeName = nodeName,
            MemberlistConfig = new MemberlistConfig
            {
                Name = nodeName,
                BindAddr = "127.0.0.1",
                BindPort = port,
                AdvertiseAddr = "127.0.0.1",
                AdvertisePort = port,

                // Aggressive test timeouts for faster convergence
                ProbeInterval = TimeSpan.FromMilliseconds(50),
                ProbeTimeout = TimeSpan.FromMilliseconds(25),
                GossipInterval = TimeSpan.FromMilliseconds(5),
                TCPTimeout = TimeSpan.FromMilliseconds(100),
                SuspicionMult = 1,

                // Require node names for strict validation
                RequireNodeNames = true
            },

            // Short intervals for testing
            ReapInterval = TimeSpan.FromSeconds(1),
            ReconnectInterval = TimeSpan.FromMilliseconds(100),
            ReconnectTimeout = TimeSpan.FromMicroseconds(1),
            TombstoneTimeout = TimeSpan.FromMicroseconds(1)
        };
    }

    /// <summary>
    /// Allocates a unique, bindable loopback port for a test transport. A monotonic counter
    /// guarantees two parallel callers never receive the same port number, and each candidate is
    /// verified to be bindable for BOTH TCP and UDP — NetTransport binds both (NetTransport.cs),
    /// and Windows reserves/excludes different port ranges per protocol (e.g. TCP 5357, UDP
    /// 50000-50059). This avoids the WSAEACCES "access forbidden" bind failures that a fixed
    /// counter (or a TCP-only probe) hits once the full parallel suite allocates enough ports.
    /// </summary>
    private static int GetFreePort()
    {
        lock (_portLock)
        {
            for (var attempt = 0; attempt < 10000; attempt++)
            {
                if (_portCounter >= 60000)
                    _portCounter = 10000;

                var port = ++_portCounter;
                if (CanBindTcpAndUdp(port))
                    return port;
            }

            throw new InvalidOperationException("Unable to find a free port for test configuration");
        }
    }

    private static bool CanBindTcpAndUdp(int port)
    {
        try
        {
            using var tcp = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            tcp.Bind(new IPEndPoint(IPAddress.Loopback, port));

            using var udp = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            udp.Bind(new IPEndPoint(IPAddress.Loopback, port));

            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    /// <summary>
    /// Waits until all Serf instances have the expected number of members.
    /// Uses polling with configurable timeout.
    /// </summary>
    /// <param name="expected">Expected number of members</param>
    /// <param name="timeout">Maximum time to wait</param>
    /// <param name="instances">Serf instances to check</param>
    /// <exception cref="TimeoutException">Thrown if timeout is exceeded</exception>
    public static async Task WaitUntilNumNodesAsync(
        int expected,
        TimeSpan timeout,
        params NSerf.Serf.Serf[] instances)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            if (instances.All(s => s.NumMembers() == expected))
            {
                return;
            }

            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        // Provide helpful error message
        var counts = string.Join(", ", instances.Select(s => s.NumMembers()));
        throw new TimeoutException(
            $"Timeout waiting for {expected} nodes. Current counts: [{counts}]");
    }

    /// <summary>
    /// Waits until all Serf instances have the expected number of members.
    /// Uses default timeout of 10 seconds.
    /// </summary>
    public static Task WaitUntilNumNodesAsync(int expected, params NSerf.Serf.Serf[] instances)
    {
        return WaitUntilNumNodesAsync(expected, TimeSpan.FromSeconds(10), instances);
    }

    /// <summary>
    /// Verifies that a specific member is in the expected status within a member list.
    /// </summary>
    /// <param name="members">List of members to search</param>
    /// <param name="name">Name of the member to find</param>
    /// <param name="status">Expected status</param>
    /// <exception cref="XunitException">Thrown if member not found or has wrong status</exception>
    public static void TestMember(List<Member> members, string name, MemberStatus status)
    {
        var member = members.FirstOrDefault(m => m.Name == name);

        if (status == MemberStatus.None)
        {
            // We expect NOT to find it
            member.Should().BeNull($"member {name} should not exist");
            return;
        }

        member.Should().NotBeNull($"member {name} should exist");
        member!.Status.Should().Be(status, $"member {name} should have status {status}");
    }

    /// <summary>
    /// Verifies that a sequence of events occurred for a specific node.
    /// Reads from event channel and filters for the target node.
    /// </summary>
    /// <param name="eventChannel">Channel to read events from</param>
    /// <param name="nodeName">Node name to filter events for</param>
    /// <param name="expectedEvents">Expected sequence of event types</param>
    /// <param name="timeout">Maximum time to wait for events</param>
    public static async Task TestEventsAsync(
        ChannelReader<IEvent> eventChannel,
        string nodeName,
        EventType[] expectedEvents,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);
        var actualEvents = new List<EventType>();

        using var cts = new CancellationTokenSource(timeout.Value);

        try
        {
            await foreach (var evt in eventChannel.ReadAllAsync(cts.Token))
            {
                if (evt is MemberEvent memberEvent)
                {
                    // Check if this event contains our target node
                    var member = memberEvent.Members.FirstOrDefault(m => m.Name == nodeName);
                    if (member != null)
                    {
                        actualEvents.Add(memberEvent.Type);

                        // Stop if we've collected enough events
                        if (actualEvents.Count >= expectedEvents.Length)
                        {
                            break;
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached, compare what we have
        }

        actualEvents.Should().Equal(expectedEvents,
            $"Expected event sequence for {nodeName}");
    }

    /// <summary>
    /// Verifies that expected user events occurred with specific names and payloads.
    /// </summary>
    public static async Task TestUserEventsAsync(
        ChannelReader<IEvent> eventChannel,
        string[] expectedNames,
        byte[][] expectedPayloads,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);
        var actualNames = new List<string>();
        var actualPayloads = new List<byte[]>();

        using var cts = new CancellationTokenSource(timeout.Value);

        try
        {
            await foreach (var evt in eventChannel.ReadAllAsync(cts.Token))
            {
                if (evt is UserEvent userEvent)
                {
                    actualNames.Add(userEvent.Name);
                    actualPayloads.Add(userEvent.Payload);

                    if (actualNames.Count >= expectedNames.Length)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached
        }

        actualNames.Should().Equal(expectedNames, "User event names should match");

        for (int i = 0; i < Math.Min(actualPayloads.Count, expectedPayloads.Length); i++)
        {
            actualPayloads[i].Should().Equal(expectedPayloads[i],
                $"User event payload {i} should match");
        }
    }

    /// <summary>
    /// Verifies that expected queries occurred with specific names and payloads.
    /// </summary>
    public static async Task TestQueryEventsAsync(
        ChannelReader<IEvent> eventChannel,
        string[] expectedNames,
        byte[][] expectedPayloads,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);
        var actualNames = new List<string>();
        var actualPayloads = new List<byte[]>();

        using var cts = new CancellationTokenSource(timeout.Value);

        try
        {
            await foreach (var evt in eventChannel.ReadAllAsync(cts.Token))
            {
                if (evt is Query query)
                {
                    actualNames.Add(query.Name);
                    actualPayloads.Add(query.Payload);

                    if (actualNames.Count >= expectedNames.Length)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached
        }

        actualNames.Should().Equal(expectedNames, "Query names should match");

        for (int i = 0; i < Math.Min(actualPayloads.Count, expectedPayloads.Length); i++)
        {
            actualPayloads[i].Should().Equal(expectedPayloads[i],
                $"Query payload {i} should match");
        }
    }

    /// <summary>
    /// Allocates a unique IP address for testing.
    /// Returns loopback addresses with incrementing last octet.
    /// </summary>
    public static IPAddress AllocateTestIP()
    {
        lock (_portLock)
        {
            // Use 127.0.x.x range for testing
            // Increment a dedicated counter to get unique IPs
            var uniqueId = ++_ipCounter;
            var octet = (uniqueId % 254) + 1;
            return IPAddress.Parse($"127.0.0.{octet}");
        }
    }

    /// <summary>
    /// Creates a test event channel for Serf configuration.
    /// Returns both reader and writer for test control.
    /// </summary>
    public static (ChannelWriter<IEvent> writer, ChannelReader<IEvent> reader) CreateTestEventChannel(
        int capacity = 1000)
    {
        var channel = Channel.CreateBounded<IEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        return (channel.Writer, channel.Reader);
    }

    /// <summary>
    /// Waits for a specific condition to become true with polling.
    /// Useful for waiting on eventual consistency conditions.
    /// </summary>
    public static Task WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        string? errorMessage = null)
    {
        return WaitForConditionAsync(condition, timeout, () => errorMessage ?? "Condition not met within timeout");
    }

    /// <summary>
    /// Waits for a condition to become true with polling. The error message factory is only
    /// evaluated on timeout, so it can describe the state observed at that moment.
    /// </summary>
    public static async Task WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        Func<string> errorMessage)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            if (condition())
            {
                return;
            }

            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException(errorMessage());
    }

    /// <summary>
    /// Waits for an asynchronous condition (e.g. an RPC response or a provider query) to become
    /// true with polling. The error message factory is only evaluated on timeout so it can
    /// describe the last observed state.
    /// </summary>
    public static async Task WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        Func<string>? errorMessage = null)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.Token.IsCancellationRequested)
        {
            if (await condition())
            {
                return;
            }

            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException(errorMessage?.Invoke() ?? "Condition not met within timeout");
    }

    /// <summary>
    /// Waits until <paramref name="serf"/> reports <paramref name="nodeName"/> with the given status.
    /// </summary>
    public static Task WaitForMemberStatusAsync(
        NSerf.Serf.Serf serf,
        string nodeName,
        MemberStatus status,
        TimeSpan timeout)
    {
        return WaitForConditionAsync(
            () => serf.Members().FirstOrDefault(m => m.Name == nodeName)?.Status == status,
            timeout,
            () => $"Timeout waiting for {nodeName} to be {status}; current status: " +
                  $"{serf.Members().FirstOrDefault(m => m.Name == nodeName)?.Status.ToString() ?? "<not a member>"}");
    }

    /// <summary>
    /// Creates multiple test Serf configurations for cluster testing.
    /// Each gets a unique name and port.
    /// </summary>
    public static List<SerfConfig> CreateTestCluster(int nodeCount)
    {
        var configs = new List<SerfConfig>();

        for (int i = 0; i < nodeCount; i++)
        {
            configs.Add(CreateTestConfig($"node-{i}"));
        }

        return configs;
    }
}

/// <summary>
/// Test-only configuration template returned by <see cref="TestHelpers.CreateTestConfig"/>.
/// Tests copy the relevant values into a real <see cref="Config"/> before creating a Serf instance.
/// (This type used to live in the NSerf library as a leftover of the early port and was never used there.)
/// </summary>
public class SerfConfig
{
    public string NodeName { get; set; } = string.Empty;
    public MemberlistConfig MemberlistConfig { get; set; } = new();
    public ILogger? Logger { get; set; }
    public IMetrics Metrics { get; set; } = NullMetrics.Instance;
    public MetricLabel[] MetricLabels { get; set; } = [];
    public TimeSpan ReapInterval { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan ReconnectInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan ReconnectTimeout { get; set; } = TimeSpan.FromHours(24);
    public TimeSpan TombstoneTimeout { get; set; } = TimeSpan.FromHours(24);
}
