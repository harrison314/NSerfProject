// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0
// Tests for tag get/set API and cluster propagation
// Addresses GitHub issue: No direct way to get current tags without knowing about LocalMember()

using System.Threading.Channels;
using FluentAssertions;
using NSerf.Serf;
using NSerf.Serf.Events;
using NSerf.Memberlist.Configuration;
using Xunit;

namespace NSerfTests.Serf;

/// <summary>
/// Tests for the tag get/set API pattern using Serf.LocalMember().Tags
/// and verification of tag propagation across cluster nodes.
/// </summary>
public class SerfTagsGetSetPropagationTest
{
    /// <summary>
    /// Test: Get current tags using LocalMember().Tags
    /// Verifies the current API pattern for retrieving tags
    /// </summary>
    [Fact]
    public async Task GetTags_UsingLocalMember_ShouldReturnCurrentTags()
    {
        // Arrange
        var initialTags = new Dictionary<string, string>
        {
            { "role", "web" },
            { "datacenter", "us-east" },
            { "version", "1.0" }
        };

        var config = new Config
        {
            NodeName = "test-node",
            Tags = initialTags,
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Get tags using LocalMember().Tags (current API pattern)
        var currentTags = serf.LocalMember().Tags;

        // Assert
        currentTags.Should().NotBeNull();
        currentTags.Should().HaveCount(3);
        currentTags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        currentTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        currentTags.Should().ContainKey("version").WhoseValue.Should().Be("1.0");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Modify a single tag by getting current tags, modifying, and setting back
    /// This is the workaround pattern users need to use
    /// </summary>
    [Fact]
    public async Task ModifySingleTag_GetModifySet_ShouldUpdateOneTag()
    {
        // Arrange
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string>
            {
                { "role", "web" },
                { "datacenter", "us-east" },
                { "version", "1.0" }
            },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Get current tags, modify one, set back
        var currentTags = new Dictionary<string, string>(serf.LocalMember().Tags);
        currentTags["role"] = "database"; // Change role from "web" to "database"
        await serf.SetTagsAsync(currentTags);

        // Assert - Verify the change
        var updatedTags = serf.LocalMember().Tags;
        updatedTags.Should().ContainKey("role").WhoseValue.Should().Be("database");
        updatedTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        updatedTags.Should().ContainKey("version").WhoseValue.Should().Be("1.0");
        updatedTags.Should().HaveCount(3, "should still have 3 tags");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Add a new tag without losing existing tags
    /// </summary>
    [Fact]
    public async Task AddNewTag_GetModifySet_ShouldPreserveExistingTags()
    {
        // Arrange
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string>
            {
                { "role", "web" },
                { "datacenter", "us-east" }
            },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Add a new tag
        var currentTags = new Dictionary<string, string>(serf.LocalMember().Tags);
        currentTags["environment"] = "production"; // Add new tag
        await serf.SetTagsAsync(currentTags);

        // Assert
        var updatedTags = serf.LocalMember().Tags;
        updatedTags.Should().HaveCount(3);
        updatedTags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        updatedTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        updatedTags.Should().ContainKey("environment").WhoseValue.Should().Be("production");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Remove a tag without affecting others
    /// </summary>
    [Fact]
    public async Task RemoveTag_GetModifySet_ShouldRemoveOnlySpecifiedTag()
    {
        // Arrange
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string>
            {
                { "role", "web" },
                { "datacenter", "us-east" },
                { "version", "1.0" }
            },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Remove "version" tag
        var currentTags = new Dictionary<string, string>(serf.LocalMember().Tags);
        currentTags.Remove("version");
        await serf.SetTagsAsync(currentTags);

        // Assert
        var updatedTags = serf.LocalMember().Tags;
        updatedTags.Should().HaveCount(2);
        updatedTags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        updatedTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        updatedTags.Should().NotContainKey("version");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Tag propagation in a 2-node cluster
    /// Verifies that tags set on one node are visible to other nodes
    /// </summary>
    [Fact]
    public async Task TagPropagation_TwoNodeCluster_ShouldPropagateTagsAcrossNodes()
    {
        // Arrange - Create 2 nodes with initial tags
        var config1 = new Config
        {
            NodeName = "node1",
            Tags = new Dictionary<string, string> { { "role", "web" } },
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
            Tags = new Dictionary<string, string> { { "role", "db" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        using var s2 = await NSerf.Serf.Serf.CreateAsync(config2);

        // Act - Join nodes
        var s2Port = config2.MemberlistConfig.BindPort;
        await s1.JoinAsync(new[] { $"127.0.0.1:{s2Port}" }, ignoreOld: false);

        // Assert - Both nodes should see each other
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), s1, s2);

        // Wait until each node sees the OTHER node's initial tags
        static string? RoleTagOf(NSerf.Serf.Serf serf, string nodeName) =>
            serf.Members().FirstOrDefault(m => m.Name == nodeName)?.Tags.GetValueOrDefault("role");

        await TestHelpers.WaitForConditionAsync(
            () => RoleTagOf(s1, "node2") == "db" && RoleTagOf(s2, "node1") == "web",
            TimeSpan.FromSeconds(10),
            () => $"initial tags not visible remotely: s1 sees node2 role='{RoleTagOf(s1, "node2")}', s2 sees node1 role='{RoleTagOf(s2, "node1")}'");

        // Each node should see its own tags via LocalMember()
        s1.LocalMember().Tags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        s2.LocalMember().Tags.Should().ContainKey("role").WhoseValue.Should().Be("db");

        // Each node should see the other's tags in Members()
        var node2FromS1 = s1.Members().FirstOrDefault(m => m.Name == "node2");
        node2FromS1.Should().NotBeNull();
        node2FromS1!.Tags.Should().ContainKey("role").WhoseValue.Should().Be("db");

        var node1FromS2 = s2.Members().FirstOrDefault(m => m.Name == "node1");
        node1FromS2.Should().NotBeNull();
        node1FromS2!.Tags.Should().ContainKey("role").WhoseValue.Should().Be("web");

        await s1.ShutdownAsync();
        await s2.ShutdownAsync();
    }

    /// <summary>
    /// Test: Tag updates are visible locally using LocalMember().Tags
    /// Verifies the get-modify-set pattern for updating tags
    /// Note: Cross-node propagation requires active gossip loop (tested in integration tests)
    /// </summary>
    [Fact]
    public async Task TagUpdate_LocallyVisible_UsingGetModifySetPattern()
    {
        // Arrange - Create node with initial tags
        var config = new Config
        {
            NodeName = "node1",
            Tags = new Dictionary<string, string> { { "role", "web" }, { "version", "1.0" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node1",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Verify initial tags via LocalMember().Tags
        var initialTags = serf.LocalMember().Tags;
        initialTags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        initialTags.Should().ContainKey("version").WhoseValue.Should().Be("1.0");

        // Act - Update tags using the get-modify-set pattern
        var updatedTags = new Dictionary<string, string>(serf.LocalMember().Tags);
        updatedTags["role"] = "api"; // Change role
        updatedTags["version"] = "2.0"; // Update version
        updatedTags["environment"] = "prod"; // Add new tag
        await serf.SetTagsAsync(updatedTags);

        // Assert - Verify updates are immediately visible via LocalMember().Tags
        var currentTags = serf.LocalMember().Tags;
        currentTags.Should().ContainKey("role").WhoseValue.Should().Be("api");
        currentTags.Should().ContainKey("version").WhoseValue.Should().Be("2.0");
        currentTags.Should().ContainKey("environment").WhoseValue.Should().Be("prod");
        currentTags.Should().HaveCount(3);

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Multiple tag updates in sequence propagate correctly
    /// </summary>
    [Fact]
    public async Task MultipleTagUpdates_InSequence_ShouldPropagateAll()
    {
        // Arrange - two nodes, so propagation of the updates can actually be observed
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string> { { "counter", "0" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };
        var observerConfig = new Config
        {
            NodeName = "observer",
            MemberlistConfig = new MemberlistConfig
            {
                Name = "observer",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);
        using var observer = await NSerf.Serf.Serf.CreateAsync(observerConfig);
        await observer.JoinAsync(new[] { $"127.0.0.1:{config.MemberlistConfig.BindPort}" }, ignoreOld: false);
        await TestHelpers.WaitUntilNumNodesAsync(2, TimeSpan.FromSeconds(10), serf, observer);

        static string? CounterSeenBy(NSerf.Serf.Serf s) =>
            s.Members().FirstOrDefault(m => m.Name == "test-node")?.Tags.GetValueOrDefault("counter");

        // Act - Perform multiple updates
        for (int i = 1; i <= 5; i++)
        {
            var tags = new Dictionary<string, string>(serf.LocalMember().Tags);
            tags["counter"] = i.ToString();
            await serf.SetTagsAsync(tags);
            await Task.Delay(50); // Small delay between updates
        }

        // Assert - Final value should be "5" locally and on the observer
        var finalTags = serf.LocalMember().Tags;
        finalTags.Should().ContainKey("counter").WhoseValue.Should().Be("5");

        await TestHelpers.WaitForConditionAsync(
            () => CounterSeenBy(observer) == "5",
            TimeSpan.FromSeconds(10),
            () => $"the observer never saw the final tag value: it sees counter='{CounterSeenBy(observer)}'");

        await serf.ShutdownAsync();
        await observer.ShutdownAsync();
    }

    /// <summary>
    /// Test: Tag propagation in 3-node cluster
    /// Verifies tags propagate across all nodes in larger cluster
    /// </summary>
    [Fact]
    public async Task TagPropagation_ThreeNodeCluster_ShouldPropagateToAllNodes()
    {
        // Arrange - Create 3 nodes
        var config1 = new Config
        {
            NodeName = "node1",
            Tags = new Dictionary<string, string> { { "role", "web" } },
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
            Tags = new Dictionary<string, string> { { "role", "api" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node2",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        var config3 = new Config
        {
            NodeName = "node3",
            Tags = new Dictionary<string, string> { { "role", "db" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "node3",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var s1 = await NSerf.Serf.Serf.CreateAsync(config1);
        using var s2 = await NSerf.Serf.Serf.CreateAsync(config2);
        using var s3 = await NSerf.Serf.Serf.CreateAsync(config3);

        // Act - Join all nodes
        var s2Port = config2.MemberlistConfig.BindPort;
        var s3Port = config3.MemberlistConfig.BindPort;

        await s1.JoinAsync(new[] { $"127.0.0.1:{s2Port}" }, ignoreOld: false);
        await s1.JoinAsync(new[] { $"127.0.0.1:{s3Port}" }, ignoreOld: false);

        // Assert - All nodes should see 3 members
        await TestHelpers.WaitUntilNumNodesAsync(3, TimeSpan.FromSeconds(10), s1, s2, s3);

        // Verify each node can see its own tags via LocalMember()
        s1.LocalMember().Tags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        s2.LocalMember().Tags.Should().ContainKey("role").WhoseValue.Should().Be("api");
        s3.LocalMember().Tags.Should().ContainKey("role").WhoseValue.Should().Be("db");

        // Update node1's tags
        var node1Tags = new Dictionary<string, string>(s1.LocalMember().Tags);
        node1Tags["status"] = "updated";
        await s1.SetTagsAsync(node1Tags);

        // Verify node1 sees its own update
        s1.LocalMember().Tags.Should().ContainKey("status").WhoseValue.Should().Be("updated");

        // Wait for propagation - the REMOTE nodes must observe node1's new tag
        // (Go: TestSerf_SetTags waits until members on the other nodes carry the tag)
        static string? StatusTagOf(NSerf.Serf.Serf serf, string nodeName) =>
            serf.Members().FirstOrDefault(m => m.Name == nodeName)?.Tags.GetValueOrDefault("status");

        await TestHelpers.WaitForConditionAsync(
            () => StatusTagOf(s2, "node1") == "updated" && StatusTagOf(s3, "node1") == "updated",
            TimeSpan.FromSeconds(10),
            () => $"node1's tag update did not propagate: node2 sees status='{StatusTagOf(s2, "node1")}', node3 sees status='{StatusTagOf(s3, "node1")}'");

        var node1FromS2 = s2.Members().First(m => m.Name == "node1");
        node1FromS2.Tags.Should().ContainKey("status").WhoseValue.Should().Be("updated");
        node1FromS2.Tags.Should().ContainKey("role").WhoseValue.Should().Be("web", "existing tags must survive the update");

        var node1FromS3 = s3.Members().First(m => m.Name == "node1");
        node1FromS3.Tags.Should().ContainKey("status").WhoseValue.Should().Be("updated");
        node1FromS3.Tags.Should().ContainKey("role").WhoseValue.Should().Be("web", "existing tags must survive the update");

        await s1.ShutdownAsync();
        await s2.ShutdownAsync();
        await s3.ShutdownAsync();
    }

    /// <summary>
    /// Test: Concurrent tag reads while updating
    /// Verifies thread-safety of LocalMember().Tags access
    /// </summary>
    [Fact]
    public async Task ConcurrentTagReads_WhileUpdating_ShouldBeThreadSafe()
    {
        // Arrange
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string> { { "counter", "0" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Concurrent reads and writes
        var readTasks = new List<Task>();
        var writeTasks = new List<Task>();

        for (int i = 0; i < 10; i++)
        {
            int iteration = i;
            
            // Writer task
            writeTasks.Add(Task.Run(async () =>
            {
                var tags = new Dictionary<string, string> { { "counter", iteration.ToString() } };
                await serf.SetTagsAsync(tags);
            }));

            // Reader task
            readTasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 10; j++)
                {
                    var tags = serf.LocalMember().Tags;
                    tags.Should().NotBeNull();
                    tags.Should().ContainKey("counter");
                }
            }));
        }

        // Wait for all tasks
        await Task.WhenAll(writeTasks);
        await Task.WhenAll(readTasks);

        // Assert - Should complete without exceptions
        var finalTags = serf.LocalMember().Tags;
        finalTags.Should().ContainKey("counter");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: Empty tags can be retrieved and set
    /// </summary>
    [Fact]
    public async Task EmptyTags_GetAndSet_ShouldWorkCorrectly()
    {
        // Arrange - Start with tags
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string> { { "role", "web" } },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Clear all tags
        await serf.SetTagsAsync(new Dictionary<string, string>());

        // Assert
        var tags = serf.LocalMember().Tags;
        tags.Should().NotBeNull();
        tags.Should().BeEmpty();

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: GetTags() convenience method returns current tags
    /// Verifies the new convenience method works correctly
    /// </summary>
    [Fact]
    public async Task GetTags_ConvenienceMethod_ShouldReturnCurrentTags()
    {
        // Arrange
        var initialTags = new Dictionary<string, string>
        {
            { "role", "web" },
            { "datacenter", "us-east" },
            { "version", "1.0" }
        };

        var config = new Config
        {
            NodeName = "test-node",
            Tags = initialTags,
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Use the new GetTags() convenience method
        var currentTags = serf.GetTags();

        // Assert
        currentTags.Should().NotBeNull();
        currentTags.Should().HaveCount(3);
        currentTags.Should().ContainKey("role").WhoseValue.Should().Be("web");
        currentTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        currentTags.Should().ContainKey("version").WhoseValue.Should().Be("1.0");

        // Verify it returns a defensive copy (modifying returned dict doesn't affect serf)
        currentTags["role"] = "modified";
        var tagsAgain = serf.GetTags();
        tagsAgain["role"].Should().Be("web", "GetTags() should return a defensive copy");

        await serf.ShutdownAsync();
    }

    /// <summary>
    /// Test: GetTags() and SetTagsAsync() work together for single tag modification
    /// Demonstrates the improved API for modifying one tag
    /// </summary>
    [Fact]
    public async Task GetTags_WithSetTags_ShouldEnableEasySingleTagModification()
    {
        // Arrange
        var config = new Config
        {
            NodeName = "test-node",
            Tags = new Dictionary<string, string>
            {
                { "role", "web" },
                { "datacenter", "us-east" },
                { "version", "1.0" }
            },
            MemberlistConfig = new MemberlistConfig
            {
                Name = "test-node",
                BindAddr = "127.0.0.1",
                BindPort = 0
            }
        };

        using var serf = await NSerf.Serf.Serf.CreateAsync(config);

        // Act - Use GetTags() to modify just one tag (improved API)
        var tags = serf.GetTags();
        tags["role"] = "database"; // Change only the role
        await serf.SetTagsAsync(tags);

        // Assert - Verify only role changed, other tags preserved
        var updatedTags = serf.GetTags();
        updatedTags.Should().ContainKey("role").WhoseValue.Should().Be("database");
        updatedTags.Should().ContainKey("datacenter").WhoseValue.Should().Be("us-east");
        updatedTags.Should().ContainKey("version").WhoseValue.Should().Be("1.0");
        updatedTags.Should().HaveCount(3);

        await serf.ShutdownAsync();
    }
}
