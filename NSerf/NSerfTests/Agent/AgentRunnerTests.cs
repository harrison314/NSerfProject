// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Net;
using System.Net.Sockets;
using NSerf.Agent;
using NSerf.Client;

namespace NSerfTests.Agent;

/// <summary>
/// Behavioural tests for the library agent runner (<see cref="AgentCommand"/>), mirroring
/// Go's cmd/serf/command/agent/command.go: one lifecycle path on top of <see cref="SerfAgent"/>,
/// logs streamed to the console through the level filter, and a clean console restore.
/// </summary>
public class AgentRunnerTests
{
    private static string GetFreeRpcAddr()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"127.0.0.1:{port}";
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(50);
        }
        return condition();
    }

    [Fact]
    public async Task RunAsync_WithRpcAddr_RunsSingleRpcServer_ClientCanListMembers()
    {
        var rpcAddr = GetFreeRpcAddr();
        var config = new AgentConfig
        {
            NodeName = "runner-rpc",
            BindAddr = "127.0.0.1:0",
            RpcAddr = rpcAddr
        };

        await using var command = new AgentCommand(config);
        using var cts = new CancellationTokenSource();
        var runTask = Task.Run(() => command.RunAsync(cts.Token));

        // The agent (and its single RPC server) must come up and stay up.
        RpcClient? client = null;
        var connected = await WaitUntilAsync(() =>
        {
            if (runTask.IsCompleted) return true; // stop polling, assert below
            try
            {
                var c = new RpcClient(new RpcConfig { Address = rpcAddr });
                c.ConnectAsync().GetAwaiter().GetResult();
                client = c;
                return true;
            }
            catch
            {
                return false;
            }
        }, TimeSpan.FromSeconds(5));

        try
        {
            runTask.IsCompleted.Should().BeFalse("the runner must keep running when RpcAddr is set (no duplicate RPC bind)");
            connected.Should().BeTrue("an RPC client must be able to connect to the configured RpcAddr");

            var members = await client!.MembersAsync();
            members.Should().ContainSingle().Which.Name.Should().Be("runner-rpc");
        }
        finally
        {
            if (client != null) await client.DisposeAsync();
            cts.Cancel();
        }

        var exitCode = await runTask;
        exitCode.Should().Be(0);
    }

    [Fact]
    public async Task RunAsync_RetryJoinExhausted_ReturnsOne_UsingAgentRetryLoop()
    {
        // Unreachable target: nothing listens on the discard port, so every attempt fails fast.
        var config = new AgentConfig
        {
            NodeName = "runner-retry",
            BindAddr = "127.0.0.1:0",
            RetryJoin = ["127.0.0.1:9"],
            RetryInterval = TimeSpan.FromMilliseconds(100),
            RetryMaxAttempts = 5
        };

        var originalOut = Console.Out;
        var captured = new ThreadSafeStringWriter();
        Console.SetOut(captured);
        try
        {
            await using var command = new AgentCommand(config);
            var exitCode = await command.RunAsync().WaitAsync(TimeSpan.FromSeconds(15));

            // The exit code and the message below come from SerfAgent's RetryJoinExhausted event,
            // i.e. the runner relies on the agent's retry-join loop (it no longer has one of its own).
            exitCode.Should().Be(1, "Go exits with 1 once the maximum retry join attempts are made");
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        captured.ToString().Should().Contain("maximum retry join attempts made, exiting");
    }

    [Fact]
    public async Task RunAsync_AtInfoLevel_PrintsAgentLogsToConsole()
    {
        var config = new AgentConfig
        {
            NodeName = "runner-info-logs",
            BindAddr = "127.0.0.1:0",
            LogLevel = "INFO"
        };

        var originalOut = Console.Out;
        var captured = new ThreadSafeStringWriter();
        Console.SetOut(captured);
        string output;
        try
        {
            await using var command = new AgentCommand(config);
            using var cts = new CancellationTokenSource();
            var runTask = Task.Run(() => command.RunAsync(cts.Token));

            // Wait for the line itself rather than for a fixed time
            await WaitUntilAsync(
                () => runTask.IsCompleted || captured.ToString().Contains("[INFO] [Agent] Started successfully"),
                TimeSpan.FromSeconds(10));
            cts.Cancel();
            (await runTask).Should().Be(0);
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        output = captured.ToString();
        output.Should().Contain("[INFO] [Agent] Started successfully", "agent log lines must be printed to the console at INFO");
    }

    [Fact]
    public async Task RunAsync_AtWarnLevel_FiltersInfoLinesFromConsole()
    {
        var config = new AgentConfig
        {
            NodeName = "runner-warn-logs",
            BindAddr = "127.0.0.1:0",
            LogLevel = "WARN"
        };

        var originalOut = Console.Out;
        var captured = new ThreadSafeStringWriter();
        Console.SetOut(captured);
        try
        {
            await using var command = new AgentCommand(config);
            using var cts = new CancellationTokenSource();
            var runTask = Task.Run(() => command.RunAsync(cts.Token));

            // The banner is written once the agent is up; the buffered log lines are released after it
            await WaitUntilAsync(
                () => runTask.IsCompleted || captured.ToString().Contains("==> Log data will now stream in as it occurs:"),
                TimeSpan.FromSeconds(10));
            cts.Cancel();
            (await runTask).Should().Be(0);
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        captured.ToString().Should().NotContain("[INFO]", "INFO lines are below the configured WARN level");
    }

    [Fact]
    public async Task DisposeAsync_RestoresOriginalConsoleOut()
    {
        var config = new AgentConfig
        {
            NodeName = "runner-console-restore",
            BindAddr = "127.0.0.1:0"
        };

        var testOriginal = Console.Out;
        var captured = new ThreadSafeStringWriter();
        Console.SetOut(captured);
        try
        {
            var beforeRun = Console.Out;

            var command = new AgentCommand(config);
            using var cts = new CancellationTokenSource();
            var runTask = Task.Run(() => command.RunAsync(cts.Token));
            await WaitUntilAsync(() => runTask.IsCompleted || command.Agent?.Serf != null, TimeSpan.FromSeconds(10));
            cts.Cancel();
            await runTask;
            await command.DisposeAsync();

            Console.Out.Should().BeSameAs(beforeRun, "the runner must restore the writer that was installed before RunAsync");
        }
        finally
        {
            Console.SetOut(testOriginal);
        }
    }
}
