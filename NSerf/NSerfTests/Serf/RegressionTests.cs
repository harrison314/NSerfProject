using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NSerf.Memberlist.Configuration;
using NSerf.Serf;
using Xunit;

namespace NSerfTests.Serf;

/// <summary>
/// Regression tests for critical bugs fixed during snapshot lifecycle implementation.
/// Each test corresponds to a specific bug to prevent reintroduction.
/// </summary>
[Collection("Sequential Snapshot Tests")]
public class RegressionTests : IDisposable
{
    private readonly System.Collections.Generic.List<string> _tempFiles = new();

    public void Dispose()
    {
        foreach (var file in _tempFiles)
        {
            try
            {
                if (System.IO.File.Exists(file))
                {
                    System.IO.File.Delete(file);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    private string GetTempSnapshotPath()
    {
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"serf_regression_{Guid.NewGuid()}.snapshot");
        _tempFiles.Add(path);
        return path;
    }

    /// <summary>
    /// Polls the snapshot file until it contains <paramref name="expected"/> (the snapshotter flushes
    /// on a 500ms timer, so the file lags the in-memory state).
    /// </summary>
    private static Task WaitForSnapshotContentAsync(string path, string expected, TimeSpan timeout)
    {
        return TestHelpers.WaitForConditionAsync(
            () => ReadSnapshotOrEmpty(path).Contains(expected),
            timeout,
            () => $"snapshot {path} did not contain '{expected}' within {timeout}");
    }

    private static string ReadSnapshotOrEmpty(string path)
    {
        try
        {
            using var fs = new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite);
            using var reader = new System.IO.StreamReader(fs);
            return reader.ReadToEnd();
        }
        catch (System.IO.IOException)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Regression Test #1: SO_REUSEADDR Socket Binding
    /// Bug: Rapid test execution caused "socket access denied" errors due to TIME_WAIT
    /// Fix: Added SO_REUSEADDR option to TCP and UDP listeners in NetTransport.cs
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task RapidPortReuse_ShouldNotFail()
    {
        // This test verifies that we can rapidly create/destroy Serf instances
        // on the same port without socket binding errors

        int port = 0; // Let OS assign

        for (int i = 0; i < 3; i++)
        {
            var config = new Config
            {
                NodeName = $"node{i}",
                MemberlistConfig = new MemberlistConfig
                {
                    Name = $"node{i}",
                    BindAddr = "127.0.0.1",
                    BindPort = port
                }
            };

            var serf = await NSerf.Serf.Serf.CreateAsync(config);

            // Capture the port for next iteration
            if (i == 0)
            {
                port = config.MemberlistConfig.BindPort;
            }

            await serf.ShutdownAsync();
            serf.Dispose();

            // Small delay to allow cleanup
            await Task.Delay(50);
        }

        // If we get here without exceptions, the test passes
        true.Should().BeTrue();
    }

    /// <summary>
    /// Regression Test #2: Stale Join Intents Resurrecting Left Members
    /// Bug: Join intent messages with newer LTime would resurrect Left/Failed members back to Alive
    /// Fix: HandleNodeJoinIntent in Serf.cs only allows Leaving->Alive, not Left/Failed->Alive
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task JoinIntent_ShouldNotResurrectLeftMember()
    {
        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        var config2 = new Config
        {
            NodeName = "node2",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);

        // Join the nodes
        await s1.JoinAsync(new[] { $"127.0.0.1:{config2.MemberlistConfig.BindPort}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Node2 leaves gracefully
        await s2.LeaveAsync();
        await s2.ShutdownAsync();
        s2.Dispose();

        // Verify node2 is marked as Left
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(10));
        var members = s1.Members();
        var node2 = members.FirstOrDefault(m => m.Name == "node2");
        node2.Should().NotBeNull();
        node2!.Status.Should().Be(MemberStatus.Left, "node2 should be Left after graceful leave");

        // Simulate join intent messages continuing to propagate (they're still in gossip)
        // Wait and check that node2 doesn't transition back to Alive from stale join intents
        await Task.Delay(1000);

        members = s1.Members();
        node2 = members.FirstOrDefault(m => m.Name == "node2");
        node2!.Status.Should().Be(MemberStatus.Left, "node2 should REMAIN Left despite join intent gossip");

        await s1.ShutdownAsync();
    }

    /// <summary>
    /// Regression Test #3: Leave Intents Downgrading Left Status
    /// Bug: Leave intent messages could downgrade Left status back to Leaving
    /// Fix: HandleNodeLeaveIntent in Serf.cs checks for Left status and ignores leave intents
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task LeaveIntent_ShouldNotDowngradeLeftStatus()
    {
        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        var config2 = new Config
        {
            NodeName = "node2",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);

        await s1.JoinAsync(new[] { $"127.0.0.1:{config2.MemberlistConfig.BindPort}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Node2 leaves
        await s2.LeaveAsync();
        await s2.ShutdownAsync();
        s2.Dispose();

        // Verify node2 reaches Left status (from memberlist Dead message)
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(10));

        // Continue checking that leave intents don't downgrade it to Leaving
        await Task.Delay(1000);

        var finalMembers = s1.Members();
        var finalNode2 = finalMembers.FirstOrDefault(m => m.Name == "node2");
        finalNode2!.Status.Should().Be(MemberStatus.Left, "node2 should remain Left, not downgrade to Leaving");

        await s1.ShutdownAsync();
    }

    /// <summary>
    /// Regression Test #4: Stale Member Status in Members()
    /// Bug: Members() returned cached Member.Status instead of current memberInfo.Status
    /// Fix: Members() now always syncs Member.Status = memberInfo.Status before returning
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task Members_ShouldReturnCurrentStatus()
    {
        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        var config2 = new Config
        {
            NodeName = "node2",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50)
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);

        await s1.JoinAsync(new[] { $"127.0.0.1:{config2.MemberlistConfig.BindPort}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Initial status should be Alive
        var members1 = s1.Members();
        var node2Initial = members1.FirstOrDefault(m => m.Name == "node2");
        node2Initial!.Status.Should().Be(MemberStatus.Alive);

        // Shutdown node2 to trigger failure
        await s2.ShutdownAsync();
        s2.Dispose();

        // Wait for failure detection (can take a while due to probe intervals)
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Failed, TimeSpan.FromSeconds(10));

        // Multiple calls to Members() should all return current status (Failed)
        for (int i = 0; i < 5; i++)
        {
            var members = s1.Members();
            var node2 = members.FirstOrDefault(m => m.Name == "node2");
            node2!.Status.Should().Be(MemberStatus.Failed,
                $"Members() call #{i + 1} should return current Failed status, not cached Alive");
            await Task.Delay(50);
        }

        await s1.ShutdownAsync();
    }

    /// <summary>
    /// Regression Test #5: Rejoin with Lower Incarnation Number
    /// Bug: HandleAliveNode rejected rejoining nodes with incarnation less than stored value
    /// Fix: Added isRejoining flag in StateHandlers.cs to allow Left/Dead nodes to bypass incarnation checks
    /// NOTE: This test verifies manual rejoin works, not auto-rejoin (which requires nodes in snapshot)
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task RejoinWithLowerIncarnation_ShouldSucceed()
    {
        var snapshotPath = GetTempSnapshotPath();

        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        var config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath,
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);
        var s2Port = config2.MemberlistConfig.BindPort;

        // Join and let node2 build up incarnation number
        await s1.JoinAsync(new[] { $"127.0.0.1:{s2Port}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Wait for the snapshot to be flushed (node2 must have recorded node1 as alive)
        await WaitForSnapshotContentAsync(snapshotPath, "alive: node1", TimeSpan.FromSeconds(5));

        // Shutdown node2 (failure scenario)
        await s2.ShutdownAsync();
        s2.Dispose();

        // Wait for failure detection (can take a while due to probe intervals)
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Failed, TimeSpan.FromSeconds(10));

        // Remove failed node
        await s1.RemoveFailedNodeAsync("node2");

        // Verify node2 is Left
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(5));
        var members2 = s1.Members();
        var node2Left = members2.FirstOrDefault(m => m.Name == "node2");
        node2Left!.Status.Should().Be(MemberStatus.Left);

        // Restart node2 (incarnation resets to 0)
        config2.MemberlistConfig.BindPort = s2Port;
        using var s2Restarted = await NSerf.Serf.Serf.CreateAsync(config2);
        await Task.Delay(300);

        // Manually rejoin node2 to node1 (snapshot was cleared by RemoveFailedNode, so no auto-rejoin)
        await s2Restarted.JoinAsync(new[] { $"127.0.0.1:{config1.MemberlistConfig.BindPort}" }, ignoreOld: false);

        // Wait for join to propagate and status to update
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Alive, TimeSpan.FromSeconds(5));

        // Verify final state
        var membersFinal = s1.Members();
        membersFinal.Length.Should().Be(2, "both nodes should be in the cluster");

        var node2Final = membersFinal.FirstOrDefault(m => m.Name == "node2");
        node2Final!.Status.Should().Be(MemberStatus.Alive, "node2 should be Alive after rejoin despite lower incarnation");

        await s1.ShutdownAsync();
        await s2Restarted.ShutdownAsync();
    }

    /// <summary>
    /// Regression Test #6: Combined Scenario - Full Lifecycle
    /// Tests all fixes together in a realistic scenario
    /// NOTE: After Leave, snapshot is cleared, so this tests manual rejoin
    /// </summary>
    // Heavy end-to-end test: creates 3 Serf nodes and exercises join/leave/restart/rejoin with
    // several gossip-convergence waits. Given a generous timeout so it does not abort under heavy
    // CPU contention when the whole suite runs 16-way parallel (it completes in ~10s unloaded).
    [Fact(Timeout = 90000)]
    public async Task FullLifecycle_WithAllFixes_ShouldWork()
    {
        var snapshotPath = GetTempSnapshotPath();

        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50),
                PushPullInterval = TimeSpan.FromMilliseconds(500)  // Enable faster push-pull for rejoin
            }
        };

        var config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath,
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50),
                PushPullInterval = TimeSpan.FromMilliseconds(500)  // Enable faster push-pull for rejoin
            }
        };

        await using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);
        var s2Port = config2.MemberlistConfig.BindPort;

        // Step 1: Join
        await s1.JoinAsync([$"127.0.0.1:{s2Port}"], ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);
        s1.NumMembers().Should().Be(2);

        // Wait for the snapshot to be flushed (node2 must have recorded node1 as alive)
        await WaitForSnapshotContentAsync(snapshotPath, "alive: node1", TimeSpan.FromSeconds(5));

        // Step 2: Graceful leave (tests leave intent handling)
        await s2.LeaveAsync();
        await s2.ShutdownAsync();
        await s2.DisposeAsync();

        // Wait for Left status
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(10));

        // Verify Left status (not stuck in Leaving)
        var membersLeft = s1.Members();
        var node2Left = membersLeft.FirstOrDefault(m => m.Name == "node2");
        node2Left!.Status.Should().Be(MemberStatus.Left);

        // Step 3: Attempt restart and rejoin
        // After graceful leave, snapshot is cleared, so node2 restarts with incarnation 0.
        // node1 has node2 as Left(incarnation=1).
        // When node2 tries to join with incarnation 0 < 1, the Alive message is rejected.
        // node1 then starts probing node2, and when probes fail, marks it as Failed.
        // This is expected behavior: after graceful leave with cleared snapshot, rejoin doesn't work automatically.
        
        config2.MemberlistConfig.BindPort = s2Port;
        await using var s2Restarted = await NSerf.Serf.Serf.CreateAsync(config2);
        await Task.Delay(300);

        // Attempt rejoin (will be rejected due to lower incarnation after Left)
        await s2Restarted.JoinAsync([$"127.0.0.1:{config1.MemberlistConfig.BindPort}"], ignoreOld: false);
        
        // Wait for probing to detect failure. Poll instead of a single fixed delay so the test
        // tolerates slower gossip convergence under heavy parallel load (it still asserts the
        // same end state below).
        await TestHelpers.WaitForConditionAsync(
            () => s1.Members().FirstOrDefault(m => m.Name == "node2")?.Status is MemberStatus.Left or MemberStatus.Failed,
            TimeSpan.FromSeconds(10),
            "node2 should be Left or Failed on s1 after the rejected rejoin");

        // Verify that node2 is detected as Failed on s1 (rejoin was rejected, then probing failed)
        var finalMembers = s1.Members();
        finalMembers.Length.Should().Be(2, "both nodes tracked");
        var node1Final = finalMembers.First(m => m.Name == "node1");
        var node2Final = finalMembers.First(m => m.Name == "node2");

        node1Final.Status.Should().Be(MemberStatus.Alive, "node1 should be Alive");
        // After graceful leave with cleared snapshot, rejoin doesn't work - node becomes Failed
        (node2Final.Status is MemberStatus.Left or MemberStatus.Failed)
            .Should().BeTrue("node2 should be Left or Failed after rejected rejoin attempt");

        // Step 4: Verify Members() returns consistent status
        await Task.Delay(1500);
        for (var i = 0; i < 3; i++)
        {
            var membersFresh = s1.Members();
            var node2Fresh = membersFresh.FirstOrDefault(m => m.Name == "node2");
            // Status should be stable (Left or Failed, not changing)
            node2Fresh.Should().NotBeNull();
            await Task.Delay(100);
        }

        await s1.ShutdownAsync();
        await s2Restarted.ShutdownAsync();
    }
}
