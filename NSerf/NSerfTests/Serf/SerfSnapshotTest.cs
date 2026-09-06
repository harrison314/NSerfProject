using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSerf.Memberlist.Configuration;
using NSerf.Serf;
using Xunit;

namespace NSerfTests.Serf;

/// <summary>
/// Integration tests for Serf snapshot and recovery functionality.
/// Tests snapshot persistence, recovery, and auto-rejoin behavior.
/// </summary>
[Collection("Sequential Snapshot Tests")]
public class SerfSnapshotTest : IDisposable
{
    private readonly List<string> _tempFiles = new();

    public void Dispose()
    {
        // Clean up temp files
        foreach (var file in _tempFiles)
        {
            try
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
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
        var path = Path.Combine(Path.GetTempPath(), $"serf_test_{Guid.NewGuid()}.snapshot");
        _tempFiles.Add(path);
        return path;
    }

    /// <summary>
    /// TestSerf_SnapshotRecovery - Verifies snapshot save/restore and auto-rejoin
    /// Port of Go test: serf_test.go lines 1770-1861
    /// </summary>
    [Fact]
    public async Task Serf_SnapshotRecovery_ShouldRestoreAndAutoRejoin()
    {
        // Arrange - Create 2-node cluster with snapshot enabled on node2
        var snapshotPath = GetTempSnapshotPath();
        
        var config1 = new Config
        {
            NodeName = "node1",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 32107,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50),
                RequireNodeNames = false
            }
        };
        
        var config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath,
            RejoinAfterLeave = true, // CRITICAL: Must be true to auto-rejoin after restart
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 32118,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50),
                RequireNodeNames = false
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        var s2 = await NSerf.Serf.Serf.CreateAsync(config2);
        var s2Port = config2.MemberlistConfig.BindPort;

        await Task.Delay(200);

        // Join nodes
        var joinAddr = $"127.0.0.1:{s2Port}";
        await s1.JoinAsync(new[] { joinAddr }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        s1.NumMembers().Should().Be(2, "should have 2 members after join");
        s2.NumMembers().Should().Be(2, "should have 2 members after join");

        // Fire a user event to test snapshot persistence
        await s1.UserEventAsync("test-event", System.Text.Encoding.UTF8.GetBytes("test"), coalesce: false);

        // Wait for the snapshotter (500ms flush interval) to persist node1 as alive before killing s2
        await WaitForSnapshotContentAsync(snapshotPath, "alive: node1", TimeSpan.FromSeconds(5));
        
        // Act - Simulate s2 failure by shutting it down
        await s2.ShutdownAsync();
        await s2.DisposeAsync();
        
        // Wait for failure detection
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Failed, TimeSpan.FromSeconds(10));

        // Verify s2 is marked as failed
        var s1Members = s1.Members();
        var s2Member = s1Members.FirstOrDefault(m => m.Name == "node2");
        s2Member.Should().NotBeNull("s2 should still be in member list");
        s2Member!.Status.Should().Be(MemberStatus.Failed, "s2 should be marked as failed");

        // Remove failed node
        await s1.RemoveFailedNodeAsync("node2");
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(5));

        // Verify s2 is marked as left
        var s1MembersAfterRemoval = s1.Members();
        var s2MemberAfterRemoval = s1MembersAfterRemoval.FirstOrDefault(m => m.Name == "node2");
        s2MemberAfterRemoval!.Status.Should().Be(MemberStatus.Left, "s2 should be marked as left after removal");

        // DEBUG: Read snapshot file to verify contents (s2 is shut down, so the file is final)
        await WaitForSnapshotContentAsync(snapshotPath, "alive: node1", TimeSpan.FromSeconds(5));
        var snapshotContents = await ReadSnapshotWithRetryAsync(snapshotPath);
        snapshotContents.Should().Contain("alive: node1", "snapshot should contain node1 as alive");
        snapshotContents.Should().NotContain("leave", "snapshot should not contain leave marker after shutdown (only LeaveAsync should write it)");

        // Act - Restart s2 from snapshot
        config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath, // Use same snapshot path
            RejoinAfterLeave = true, // CRITICAL: Must be true to auto-rejoin after restart
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = s2Port, // Use same port
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50),
                RequireNodeNames = false
            }
        };

        await using var s2Restarted = await NSerf.Serf.Serf.CreateAsync(config2);
        
        // Wait for auto-rejoin from BOTH perspectives
        // s2 auto-rejoins from snapshot, then s1 receives NotifyJoin callback
        static bool SeesAlive(NSerf.Serf.Serf serf, string name)
        {
            var members = serf.Members();
            return members.Length == 2 && members.Any(m => m.Name == name && m.Status == MemberStatus.Alive);
        }

        await TestHelpers.WaitForConditionAsync(
            () => SeesAlive(s2Restarted, "node1") && SeesAlive(s1, "node2"),
            TimeSpan.FromSeconds(15),
            () => $"s2 should auto-rejoin from snapshot. s2HasNode1={SeesAlive(s2Restarted, "node1")}, s1HasNode2={SeesAlive(s1, "node2")}");
        
        var s1MembersAfterRejoin = s1.Members();
        var s2MemberAfterRejoin = s1MembersAfterRejoin.FirstOrDefault(m => m.Name == "node2");
        s2MemberAfterRejoin.Should().NotBeNull();
        s2MemberAfterRejoin!.Status.Should().Be(MemberStatus.Alive, "s2 should be alive after rejoin");

        var s2Members = s2Restarted.Members();
        var s1Member = s2Members.FirstOrDefault(m => m.Name == "node1");
        s1Member.Should().NotBeNull();
        s1Member!.Status.Should().Be(MemberStatus.Alive, "s1 should be alive from s2's perspective");

        await s1.ShutdownAsync();
        await s2Restarted.ShutdownAsync();
    }

    /// <summary>
    /// TestSerf_Leave_SnapshotRecovery - Verifies that leaving prevents auto-rejoin
    /// Port of Go test: serf_test.go lines 1863-1940
    /// </summary>
    [Fact]
    public async Task Serf_Leave_SnapshotRecovery_ShouldNotAutoRejoin()
    {
        // Arrange - Create 2-node cluster with snapshot enabled on node2
        var snapshotPath = GetTempSnapshotPath();
        
        var config1 = new Config
        {
            NodeName = "node1",
            ReapInterval = TimeSpan.FromSeconds(30), // Longer reap interval
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50)
            }
        };
        
        var config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath,
            ReapInterval = TimeSpan.FromSeconds(30),
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
        var s2Port = config2.MemberlistConfig.BindPort;

        await Task.Delay(200);

        // Join nodes
        var joinAddr = $"127.0.0.1:{s2Port}";
        await s1.JoinAsync(new[] { joinAddr }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        s1.NumMembers().Should().Be(2);
        s2.NumMembers().Should().Be(2);

        // Act - Explicitly leave (not just shutdown)
        await s2.LeaveAsync();
        await s2.ShutdownAsync();
        s2.Dispose();

        // Verify s2 is marked as left
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(10));

        // Act - Restart s2 from snapshot (which contains leave marker)
        config2 = new Config
        {
            NodeName = "node2",
            SnapshotPath = snapshotPath,
            ReapInterval = TimeSpan.FromSeconds(30),
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = s2Port,
                ProbeInterval = TimeSpan.FromMilliseconds(100),
                ProbeTimeout = TimeSpan.FromMilliseconds(50)
            }
        };

        using var s2Restarted = await NSerf.Serf.Serf.CreateAsync(config2);

        // Wait and verify s2 did NOT auto-rejoin
        await Task.Delay(1000);

        // Assert - s2 should NOT have rejoined
        s2Restarted.NumMembers().Should().Be(1, "s2 should not auto-rejoin after leave");

        var s1MembersFinal = s1.Members();
        var s2MemberFinal = s1MembersFinal.FirstOrDefault(m => m.Name == "node2");
        s2MemberFinal!.Status.Should().Be(MemberStatus.Left, "s2 should still be left from s1's perspective");

        await s1.ShutdownAsync();
        await s2Restarted.ShutdownAsync();
    }

    /// <summary>
    /// Tests that RejoinAfterLeave=true allows rejoining after leave
    /// </summary>
    [Fact]
    public async Task Serf_RejoinAfterLeave_ShouldAutoRejoin()
    {
        // Arrange
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
            RejoinAfterLeave = true, // This is the key difference
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

        await Task.Delay(200);

        // Join
        await s1.JoinAsync(new[] { $"127.0.0.1:{s2Port}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Leave and shutdown
        await s2.LeaveAsync();
        await s2.ShutdownAsync();
        s2.Dispose();
        await TestHelpers.WaitForMemberStatusAsync(s1, "node2", MemberStatus.Left, TimeSpan.FromSeconds(10));

        // Restart with RejoinAfterLeave=true
        config2.MemberlistConfig.BindPort = s2Port;
        using var s2Restarted = await NSerf.Serf.Serf.CreateAsync(config2);

        // Should auto-rejoin even after leave
        await TestHelpers.WaitForConditionAsync(() => s2Restarted.NumMembers() > 1, TimeSpan.FromSeconds(5),
            "s2 should have auto-rejoined node1 from its snapshot after restart");

        // With RejoinAfterLeave, the snapshot should still have node1's address
        // so s2 should attempt to rejoin
        s2Restarted.NumMembers().Should().BeGreaterThan(1, "should have attempted to rejoin");

        await s1.ShutdownAsync();
        await s2Restarted.ShutdownAsync();
    }

    /// <summary>
    /// Polls the snapshot file until it contains <paramref name="expected"/> (the snapshotter flushes
    /// on a 500ms timer, so the file lags the in-memory state).
    /// </summary>
    private static Task WaitForSnapshotContentAsync(string path, string expected, TimeSpan timeout)
    {
        return TestHelpers.WaitForConditionAsync(
            () =>
            {
                try
                {
                    using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    using var reader = new StreamReader(fs);
                    return reader.ReadToEnd().Contains(expected);
                }
                catch (IOException)
                {
                    return false;
                }
            },
            timeout,
            () => $"snapshot {path} did not contain '{expected}' within {timeout}");
    }

    private static async Task<string> ReadSnapshotWithRetryAsync(string path, int maxRetries = 10)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new StreamReader(fs);
                return await reader.ReadToEndAsync();
            }
            catch (IOException) when (i < maxRetries - 1)
            {
                await Task.Delay(100);
            }
        }
        throw new IOException($"Could not read snapshot file after {maxRetries} attempts");
    }
}
