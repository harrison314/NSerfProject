// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0
// UpdateNode() Port - TDD Tests
// Based on: github.com/hashicorp/memberlist/memberlist_test.go - TestMemberlist_UpdateNode


using Microsoft.Extensions.Logging.Abstractions;
using NSerf.Memberlist.Configuration;
using NSerf.Memberlist.Delegates;
using NSerf.Memberlist.Messages;
using NSerf.Memberlist.State;
using NSerf.Memberlist.Transport;
using NSerfTests.Serf;


namespace NSerfTests.Memberlist;

/// <summary>
/// TDD tests for UpdateNode() functionality.
/// These tests are written BEFORE implementation to guide development.
/// </summary>
public class UpdateNodeTests : IDisposable
{
    private readonly List<NSerf.Memberlist.Memberlist> _memberlists = new();

    public void Dispose()
    {
        foreach (var ml in _memberlists)
        {
            try
            {
                ml?.ShutdownAsync().Wait();
                ml?.Dispose();
            }
            catch
            {
                // Ignore disposal errors in tests
            }
        }
        
        // Give sockets time to clean up
        Thread.Sleep(100);
    }

    /// <summary>
    /// Helper: Creates a test memberlist with custom delegate
    /// </summary>
    private NSerf.Memberlist.Memberlist CreateTestMemberlist(
        string nodeName = "node1",
        IDelegate? customDelegate = null)
    {
        var config = MemberlistConfig.DefaultLANConfig();
        config.Name = nodeName;
        config.BindAddr = "127.0.0.1";
        config.BindPort = 0; // Auto-assign
        // No explicit AdvertiseAddr: with one set, memberlist advertises Config.AdvertisePort (7946 by
        // default) rather than the OS-assigned port, and every gossip/probe would go to the wrong port.
        config.Logger = NullLogger.Instance;
        config.Delegate = customDelegate;
        
        // Fast timings for tests
        config.ProbeInterval = TimeSpan.FromMilliseconds(100);
        config.ProbeTimeout = TimeSpan.FromMilliseconds(50);
        config.GossipInterval = TimeSpan.FromMilliseconds(50);

        // Create transport with retries for port binding issues
        NetTransport? transport = null;
        for (int i = 0; i < 3; i++)
        {
            try
            {
                var transportConfig = new NetTransportConfig
                {
                    BindAddrs = new List<string> { "127.0.0.1" },
                    BindPort = 0, // Ephemeral port
                    Logger = NullLogger.Instance
                };
                transport = NetTransport.Create(transportConfig);
                break;
            }
            catch (System.Net.Sockets.SocketException)
            {
                if (i == 2) throw;
                Thread.Sleep(50); // Brief delay before retry
            }
        }
        config.Transport = transport!;

        var ml = NSerf.Memberlist.Memberlist.Create(config);
        _memberlists.Add(ml);
        return ml;
    }

    /// <summary>
    /// Helper: Creates a 2-node cluster for testing
    /// </summary>
    private async Task<(NSerf.Memberlist.Memberlist ml1, NSerf.Memberlist.Memberlist ml2)> CreateTwoNodeCluster()
    {
        var ml1 = CreateTestMemberlist("node1");
        var ml2 = CreateTestMemberlist("node2");

        // Wait for both to initialize and start listening
        await Task.Delay(200);

        // Join ml1 to ml2 using the actual bound port from config
        var ml2Port = ml2.Config.BindPort;
        var joinAddr = $"127.0.0.1:{ml2Port}";
        
        using var joinCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var (numJoined, error) = await ml1.JoinAsync(new[] { joinAddr }, joinCts.Token);
        
        error.Should().BeNull("join should not error");
        numJoined.Should().BeGreaterThan(0, "join should succeed");

        // Wait for cluster to converge
        await WaitForTwoMembersAsync(ml1, ml2);

        return (ml1, ml2);
    }

    #region Test 1: Basic Metadata Update

    /// <summary>
    /// Test 1: UpdateNode with metadata change should broadcast to cluster
    /// Maps to: TestMemberlist_UpdateNode in Go
    /// </summary>
    [Fact]
    public async Task UpdateNode_WithMetadataChange_ShouldBroadcastToCluster()
    {
        // Arrange: Create 2-node cluster with trackable metadata
        var metadata1 = new byte[] { 0x01 };
        var metadata2 = new byte[] { 0x02 };
        var currentMeta = metadata1;

        var delegate1 = new TestDelegate(() => currentMeta);
        var ml1 = CreateTestMemberlist("node1", delegate1);
        var ml2 = CreateTestMemberlist("node2");

        await Task.Delay(200);

        // Join using actual bound port
        var ml2Port = ml2.Config.BindPort;
        using var joinCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var (numJoined, error) = await ml1.JoinAsync(new[] { $"127.0.0.1:{ml2Port}" }, joinCts.Token);
        
        error.Should().BeNull();
        numJoined.Should().BeGreaterThan(0);
        await WaitForTwoMembersAsync(ml1, ml2);

        // Verify initial metadata
        ml1.LocalNode.Meta.Should().BeEquivalentTo(metadata1);
        await TestHelpers.WaitForConditionAsync(
            () => MetaSeenBy(ml2, "node1") is { } meta && meta.SequenceEqual(metadata1),
            TimeSpan.FromSeconds(5), "ml2 never learned node1's initial metadata");

        // Act: Change metadata and call UpdateNode
        currentMeta = metadata2;
        await ml1.UpdateNodeAsync(TimeSpan.FromSeconds(1));

        // Assert: ml1 should have new metadata
        ml1.LocalNode.Meta.Should().BeEquivalentTo(metadata2, "local node should reflect new metadata");

        // Verify ml1 can see itself with new metadata
        var ml1ViewFromMl1 = ml1.Members().FirstOrDefault(m => m.Name == "node1");
        ml1ViewFromMl1.Should().NotBeNull("node1 should see itself");
        ml1ViewFromMl1!.Meta.Should().BeEquivalentTo(metadata2, "node1 should see its own updated metadata");

        // Assert: the REMOTE node must observe the broadcast alive message carrying the new metadata
        // (Go: TestMemberlist_UpdateNode checks the delegate meta; the broadcast is what makes it visible cluster-wide)
        await TestHelpers.WaitForConditionAsync(
            () => MetaSeenBy(ml2, "node1") is { } meta && meta.SequenceEqual(metadata2),
            TimeSpan.FromSeconds(5),
            () => $"node1's metadata update was not broadcast to node2 (node2 sees [{string.Join(",", MetaSeenBy(ml2, "node1") ?? [])}])");

        var ml1ViewFromMl2 = ml2.Members().First(m => m.Name == "node1");
        ml1ViewFromMl2.Meta.Should().BeEquivalentTo(metadata2, "node2 should see node1's updated metadata");
    }

    #endregion

    #region Test 2: Incarnation Increment

    /// <summary>
    /// Test 2: UpdateNode should increment incarnation number
    /// </summary>
    [Fact]
    public async Task UpdateNode_ShouldIncrementIncarnation()
    {
        // Arrange
        var ml = CreateTestMemberlist("node1");
        await Task.Delay(100);

        var incarnationBefore = ml.Incarnation;

        // Act
        await ml.UpdateNodeAsync(TimeSpan.FromSeconds(1));

        // Assert
        var incarnationAfter = ml.Incarnation;
        incarnationAfter.Should().Be(incarnationBefore + 1, "incarnation should increment by 1");
    }

    #endregion

    #region Test 3: Same Metadata Still Broadcasts

    /// <summary>
    /// Test 3: UpdateNode should broadcast even if metadata hasn't changed
    /// </summary>
    [Fact]
    public async Task UpdateNode_WithSameMetadata_ShouldStillBroadcast()
    {
        // Arrange: 2-node cluster; node1's metadata never changes
        var metadata = new byte[] { 0x42 };
        var delegate1 = new TestDelegate(() => metadata);
        var ml1 = CreateTestMemberlist("node1", delegate1);
        var ml2 = CreateTestMemberlist("node2");
        await Task.Delay(200);

        using var joinCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var (numJoined, error) = await ml1.JoinAsync(new[] { $"127.0.0.1:{ml2.Config.BindPort}" }, joinCts.Token);
        error.Should().BeNull();
        numJoined.Should().BeGreaterThan(0);
        await WaitForTwoMembersAsync(ml1, ml2);

        var incarnation1 = ml1.Incarnation;

        // Act: Call UpdateNode twice with the same metadata; each call must reach node2 (Go: UpdateNode
        // always bumps the incarnation and broadcasts, even when the metadata did not change)
        await ml1.UpdateNodeAsync(TimeSpan.FromSeconds(1));
        var incarnation2 = ml1.Incarnation;
        await TestHelpers.WaitForConditionAsync(
            () => ml2.NodeMap.TryGetValue("node1", out var st) && st.Incarnation == incarnation2,
            TimeSpan.FromSeconds(5),
            () => $"node2 never observed node1's incarnation {incarnation2} (sees {(ml2.NodeMap.TryGetValue("node1", out var s) ? s.Incarnation.ToString() : "<none>")})");

        await ml1.UpdateNodeAsync(TimeSpan.FromSeconds(1));
        var incarnation3 = ml1.Incarnation;
        await TestHelpers.WaitForConditionAsync(
            () => ml2.NodeMap.TryGetValue("node1", out var st) && st.Incarnation == incarnation3,
            TimeSpan.FromSeconds(5),
            () => $"node2 never observed node1's incarnation {incarnation3} (sees {(ml2.NodeMap.TryGetValue("node1", out var s) ? s.Incarnation.ToString() : "<none>")})");

        // Assert: Both calls should increment incarnation (proving broadcast happened)
        incarnation2.Should().Be(incarnation1 + 1);
        incarnation3.Should().Be(incarnation2 + 1);
    }

    #endregion

    #region Test 4: Timeout Behavior

    /// <summary>
    /// Test 4: UpdateNode with single node (no other nodes) should not wait
    /// </summary>
    [Fact]
    public async Task UpdateNode_SingleNode_ShouldNotWaitForBroadcast()
    {
        // Arrange: Single node (no other nodes to wait for)
        var ml = CreateTestMemberlist("node1");
        await Task.Delay(100);

        // Act: Call UpdateNode with short timeout
        var startTime = DateTime.UtcNow;
        await ml.UpdateNodeAsync(TimeSpan.FromMilliseconds(100));
        var elapsed = DateTime.UtcNow - startTime;

        // Assert: Should return quickly (no nodes to wait for)
        elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(150), 
            "single node should not wait for broadcast confirmation");
    }

    /// <summary>
    /// Test 4b: UpdateNode with multi-node cluster should complete successfully
    /// </summary>
    [Fact]
    public async Task UpdateNode_MultiNode_ShouldWaitForBroadcast()
    {
        // Arrange: 2-node cluster
        var (ml1, ml2) = await CreateTwoNodeCluster();

        // Act: Call UpdateNode - should complete
        var startTime = DateTime.UtcNow;
        await ml1.UpdateNodeAsync(TimeSpan.FromSeconds(2));
        var elapsed = DateTime.UtcNow - startTime;

        // Assert: Should complete (either immediately if broadcast queued, or after wait)
        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2.5), "should complete within reasonable time");
        
        // Verify incarnation was incremented (core functionality)
        ml1.Incarnation.Should().BeGreaterThan(0, "incarnation should have incremented");

        // The broadcast that UpdateNode waited for must reach the other node: ml2 sees node1's new incarnation
        var expectedIncarnation = ml1.Incarnation;
        await TestHelpers.WaitForConditionAsync(
            () => ml2.Members().FirstOrDefault(m => m.Name == "node1") is { } n && ml2.NodeMap.TryGetValue("node1", out var st) && st.Incarnation == expectedIncarnation,
            TimeSpan.FromSeconds(5),
            () => $"node2 never observed node1's incarnation {expectedIncarnation} (sees {(ml2.NodeMap.TryGetValue("node1", out var s) ? s.Incarnation.ToString() : "<none>")})");
    }

    #endregion

    #region Test 5: Metadata Size Limit

    /// <summary>
    /// Test 5: UpdateNode with oversized metadata should throw exception
    /// </summary>
    [Fact]
    public async Task UpdateNode_WithOversizedMetadata_ShouldThrow()
    {
        // Arrange: Delegate that returns metadata exceeding limit
        var hugeMetadata = new byte[MessageConstants.MetaMaxSize + 100];
        Array.Fill(hugeMetadata, (byte)0xFF);

        var delegateWithHugeMeta = new TestDelegate(() => hugeMetadata);
        var ml = CreateTestMemberlist("node1", delegateWithHugeMeta);
        await Task.Delay(100);

        // Act & Assert
        var act = async () => await ml.UpdateNodeAsync(TimeSpan.FromSeconds(1));
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*exceeds*");
    }

    #endregion

    #region Test 6: Null Delegate

    /// <summary>
    /// Test 6: UpdateNode with null delegate should use empty metadata
    /// </summary>
    [Fact]
    public async Task UpdateNode_WithNullDelegate_ShouldUseEmptyMetadata()
    {
        // Arrange: No delegate
        var ml = CreateTestMemberlist("node1", customDelegate: null);
        await Task.Delay(100);

        // Act
        await ml.UpdateNodeAsync(TimeSpan.FromSeconds(1));

        // Assert: Should have empty metadata
        ml.LocalNode.Meta.Should().BeEmpty("null delegate should result in empty metadata");
    }

    #endregion

    #region Test 7: Concurrent Updates

    /// <summary>
    /// Test 7: Concurrent UpdateNode calls should handle race conditions safely
    /// </summary>
    [Fact]
    public async Task UpdateNode_ConcurrentCalls_ShouldHandleRaceConditions()
    {
        // Arrange
        var ml = CreateTestMemberlist("node1");
        await Task.Delay(100);

        var startIncarnation = ml.Incarnation;

        // Act: Call UpdateNode concurrently from multiple threads
        var tasks = new Task[5];
        for (int i = 0; i < 5; i++)
        {
            tasks[i] = Task.Run(async () => await ml.UpdateNodeAsync(TimeSpan.FromSeconds(1)));
        }

        await Task.WhenAll(tasks);

        // Assert: Incarnation should be incremented correctly (no race condition)
        var finalIncarnation = ml.Incarnation;
        finalIncarnation.Should().Be(startIncarnation + 5, 
            "incarnation should increment exactly 5 times despite concurrent calls");
    }

    #endregion

    #region Test 8: Event Delegate Integration

    /// <summary>
    /// Test 8: UpdateNode should trigger NotifyUpdate on remote nodes
    /// </summary>
    [Fact]
    public async Task UpdateNode_ShouldTriggerNotifyUpdateOnRemoteNodes()
    {
        // Arrange: Create 2-node cluster with event tracking
        var updateNotifications = new List<string>();
        var eventDelegate = new TestEventDelegate(updateNotifications);

        var metadata1 = new byte[] { 0x01 };
        var metadata2 = new byte[] { 0x02 };
        var currentMeta = metadata1;

        var delegate1 = new TestDelegate(() => currentMeta);
        
        var ml1 = CreateTestMemberlist("node1", delegate1);
        var ml2 = CreateTestMemberlist("node2");
        
        // Only the REMOTE node tracks NotifyUpdate so the assertion cannot be satisfied by node1's own update
        ml2.Config.Events = eventDelegate;

        await Task.Delay(200);

        // Join using actual bound port
        var ml2Port = ml2.Config.BindPort;
        using var joinCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ml1.JoinAsync(new[] { $"127.0.0.1:{ml2Port}" }, joinCts.Token);
        await WaitForTwoMembersAsync(ml1, ml2);
        await TestHelpers.WaitForConditionAsync(
            () => MetaSeenBy(ml2, "node1") is { } meta && meta.SequenceEqual(metadata1),
            TimeSpan.FromSeconds(5), "ml2 never learned node1's initial metadata");

        lock (updateNotifications) updateNotifications.Clear(); // Clear join notifications

        // Act: Update metadata on node1
        currentMeta = metadata2;
        await ml1.UpdateNodeAsync(TimeSpan.FromSeconds(1));

        // Assert: node2's event delegate receives NotifyUpdate for node1 (Go: aliveNode -> NotifyUpdate on meta change)
        await TestHelpers.WaitForConditionAsync(
            () => { lock (updateNotifications) return updateNotifications.Contains("node1"); },
            TimeSpan.FromSeconds(5),
            "node2 never received NotifyUpdate for node1's metadata change");

        ml1.Incarnation.Should().BeGreaterThan(0, "incarnation should have incremented");
        ml1.LocalNode.Meta.Should().BeEquivalentTo(metadata2, "local metadata should be updated");
        MetaSeenBy(ml2, "node1").Should().BeEquivalentTo(metadata2, "node2 should see node1's updated metadata");
    }

    #endregion

    #region Test Helpers

    private static Task WaitForTwoMembersAsync(NSerf.Memberlist.Memberlist ml1, NSerf.Memberlist.Memberlist ml2)
    {
        return TestHelpers.WaitForConditionAsync(
            () => ml1.NumMembers() == 2 && ml2.NumMembers() == 2,
            TimeSpan.FromSeconds(5),
            () => $"cluster did not converge: ml1={ml1.NumMembers()}, ml2={ml2.NumMembers()}");
    }

    private static byte[]? MetaSeenBy(NSerf.Memberlist.Memberlist observer, string nodeName) =>
        observer.Members().FirstOrDefault(m => m.Name == nodeName)?.Meta;

    /// <summary>
    /// Test delegate that provides dynamic metadata
    /// </summary>
    private class TestDelegate : IDelegate
    {
        private readonly Func<byte[]> _metadataProvider;

        public TestDelegate(Func<byte[]> metadataProvider)
        {
            _metadataProvider = metadataProvider;
        }

        public byte[] NodeMeta(int limit)
        {
            var meta = _metadataProvider();
            // Simulate Go panic if over limit
            if (meta.Length > limit)
            {
                return meta; // Let UpdateNode handle the check
            }
            return meta;
        }

        public void NotifyMsg(ReadOnlySpan<byte> msg) { }
        public List<byte[]> GetBroadcasts(int overhead, int limit) => new List<byte[]>();
        public byte[] LocalState(bool join) => Array.Empty<byte>();
        public void MergeRemoteState(ReadOnlySpan<byte> buf, bool join) { }
    }

    /// <summary>
    /// Test event delegate that tracks update notifications
    /// </summary>
    private class TestEventDelegate : IEventDelegate
    {
        private readonly List<string> _updateNotifications;

        public TestEventDelegate(List<string> updateNotifications)
        {
            _updateNotifications = updateNotifications;
        }

        public void NotifyJoin(Node node)
        {
            // Track joins if needed
        }

        public void NotifyLeave(Node node)
        {
            // Track leaves if needed
        }

        public void NotifyUpdate(Node node)
        {
            lock (_updateNotifications)
            {
                _updateNotifications.Add(node.Name);
            }
        }
    }

    #endregion
}
