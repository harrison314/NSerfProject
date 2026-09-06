// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using NSerf.Agent;

namespace NSerfTests.Agent;

/// <summary>
/// Signal semantics of the agent runner, driven through <see cref="AgentCommand.SendSignal"/>
/// (Go's handleSignals in command.go): first SIGINT leaves gracefully unless SkipLeaveOnInt,
/// SIGTERM leaves gracefully only with LeaveOnTerm, a second signal forces, SIGHUP reloads.
/// </summary>
public class AgentRunnerSignalTests
{
    private static AgentConfig NewConfig(string name) => new()
    {
        NodeName = name,
        BindAddr = "127.0.0.1:0"
    };

    private static async Task<(AgentCommand command, Task<int> runTask)> StartAsync(AgentConfig config)
    {
        var command = new AgentCommand(config);
        var runTask = Task.Run(() => command.RunAsync());

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (command.Agent?.Serf == null && !runTask.IsCompleted && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        // Give the runner a moment to reach its wait loop after the agent is up
        await Task.Delay(200);
        command.Agent.Should().NotBeNull();
        return (command, runTask);
    }

    [Fact]
    public async Task SendSignal_SigTerm_WithoutLeaveOnTerm_ForcesShutdown_ReturnsOne()
    {
        var config = NewConfig("sig-term-force");
        config.LeaveOnTerm = false;

        var (command, runTask) = await StartAsync(config);
        await using (command)
        {
            command.SendSignal(Signal.SIGTERM);
            var exitCode = await runTask.WaitAsync(TimeSpan.FromSeconds(15));
            exitCode.Should().Be(1, "SIGTERM without LeaveOnTerm is a forced shutdown");
        }
    }

    [Fact]
    public async Task SendSignal_SigTerm_WithLeaveOnTerm_LeavesGracefully_ReturnsZero()
    {
        var config = NewConfig("sig-term-graceful");
        config.LeaveOnTerm = true;

        var (command, runTask) = await StartAsync(config);
        await using (command)
        {
            command.SendSignal(Signal.SIGTERM);
            var exitCode = await runTask.WaitAsync(TimeSpan.FromSeconds(15));
            exitCode.Should().Be(0, "SIGTERM with LeaveOnTerm leaves the cluster gracefully");
        }
    }

    [Fact]
    public async Task SendSignal_SigInt_LeavesGracefullyByDefault_ReturnsZero()
    {
        var (command, runTask) = await StartAsync(NewConfig("sig-int-graceful"));
        await using (command)
        {
            command.SendSignal(Signal.SIGINT);
            var exitCode = await runTask.WaitAsync(TimeSpan.FromSeconds(15));
            exitCode.Should().Be(0);
        }
    }

    [Fact]
    public async Task SendSignal_SigInt_WithSkipLeaveOnInt_ForcesShutdown_ReturnsOne()
    {
        var config = NewConfig("sig-int-force");
        config.SkipLeaveOnInt = true;

        var (command, runTask) = await StartAsync(config);
        await using (command)
        {
            command.SendSignal(Signal.SIGINT);
            var exitCode = await runTask.WaitAsync(TimeSpan.FromSeconds(15));
            exitCode.Should().Be(1, "SIGINT with SkipLeaveOnInt is a forced shutdown");
        }
    }

    [Fact]
    public async Task SendSignal_SigHup_InvokesReloadHandlerWithAgent_AndKeepsRunning()
    {
        var (command, runTask) = await StartAsync(NewConfig("sig-hup-reload"));
        await using (command)
        {
            var reloaded = new TaskCompletionSource<SerfAgent>(TaskCreationOptions.RunContinuationsAsynchronously);
            command.ReloadHandler = (agent, _) =>
            {
                reloaded.TrySetResult(agent);
                return Task.CompletedTask;
            };

            command.SendSignal(Signal.SIGHUP);

            var agent = await reloaded.Task.WaitAsync(TimeSpan.FromSeconds(5));
            agent.Should().BeSameAs(command.Agent);
            runTask.IsCompleted.Should().BeFalse("SIGHUP only reloads; the agent keeps running");

            // SIGHUP must not count as a shutdown signal: the next SIGINT is still the first (graceful) one
            command.SendSignal(Signal.SIGINT);
            (await runTask.WaitAsync(TimeSpan.FromSeconds(15))).Should().Be(0);
        }
    }

    private static async Task<SerfAgent> StartPeerAsync(string name)
    {
        var peer = new SerfAgent(new AgentConfig { NodeName = name, BindAddr = "127.0.0.1:0" });
        await peer.StartAsync();
        return peer;
    }

    private static string AddressOf(SerfAgent agent)
    {
        var local = agent.Serf!.LocalMember();
        return $"{local.Addr}:{local.Port}";
    }

    private static Task WaitForStatusAsync(SerfAgent observer, string node, NSerf.Serf.MemberStatus status, TimeSpan timeout) =>
        NSerfTests.Serf.TestHelpers.WaitForMemberStatusAsync(observer.Serf!, node, status, timeout);

    [Fact]
    public async Task SendSignal_SigTerm_WithoutLeaveOnTerm_DoesNotLeave_PeerDetectsFailure()
    {
        await using var peer = await StartPeerAsync("force-peer");
        var config = NewConfig("force-node");
        config.LeaveOnTerm = false;
        config.StartJoin = [AddressOf(peer)];

        var (command, runTask) = await StartAsync(config);
        await using (command)
        {
            await WaitForStatusAsync(peer, "force-node", NSerf.Serf.MemberStatus.Alive, TimeSpan.FromSeconds(10));

            command.SendSignal(Signal.SIGTERM);
            (await runTask.WaitAsync(TimeSpan.FromSeconds(15))).Should().Be(1);

            // Go: a forced exit calls agent.Shutdown() without leaving, so the cluster detects a failure
            // (probe + suspicion timeout) rather than a graceful leave.
            await WaitForStatusAsync(peer, "force-node", NSerf.Serf.MemberStatus.Failed, TimeSpan.FromSeconds(30));
        }
    }

    [Fact]
    public async Task SendSignal_SigTerm_WithLeaveOnTerm_Leaves_PeerSeesNodeLeft()
    {
        await using var peer = await StartPeerAsync("leave-peer");
        var config = NewConfig("leave-node");
        config.LeaveOnTerm = true;
        config.StartJoin = [AddressOf(peer)];

        var (command, runTask) = await StartAsync(config);
        await using (command)
        {
            await WaitForStatusAsync(peer, "leave-node", NSerf.Serf.MemberStatus.Alive, TimeSpan.FromSeconds(10));

            command.SendSignal(Signal.SIGTERM);
            (await runTask.WaitAsync(TimeSpan.FromSeconds(15))).Should().Be(0);

            await WaitForStatusAsync(peer, "leave-node", NSerf.Serf.MemberStatus.Left, TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task SerfAgent_RetryJoinExhausted_IsRaisedAfterMaxAttempts()
    {
        var config = new AgentConfig
        {
            NodeName = "retry-exhausted-event",
            BindAddr = "127.0.0.1:0",
            RetryJoin = ["127.0.0.1:9"],
            RetryInterval = TimeSpan.FromMilliseconds(50),
            RetryMaxAttempts = 2
        };

        await using var agent = new SerfAgent(config);
        var exhausted = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        agent.RetryJoinExhausted += attempts => exhausted.TrySetResult(attempts);

        await agent.StartAsync();

        var attempts = await exhausted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        attempts.Should().Be(2);
        agent.Serf.Should().NotBeNull("retry exhaustion itself does not stop the agent; the runner decides");
    }
}
