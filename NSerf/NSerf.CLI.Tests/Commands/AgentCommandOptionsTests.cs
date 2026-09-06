// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using NSerf.CLI.Commands;
using NSerf.CLI.Tests.Helpers;

namespace NSerf.CLI.Tests.Commands;

/// <summary>
/// Tests for the agent command options that map to Go's cmd/serf/command/agent/command.go:
/// -discover, -log-level, -retry-join/-retry-interval/-retry-max, -snapshot and -rejoin,
/// and for the agent logs being streamed to the console at the configured level.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Sequential")]
public class AgentCommandOptionsTests
{
    private static int _counter;

    private static string NextNodeName() => $"opts-node-{Interlocked.Increment(ref _counter)}";

    /// <summary>
    /// Runs the CLI agent command with captured console output. The agent is cancelled after
    /// <paramref name="runFor"/> unless it exits by itself first.
    /// </summary>
    private static async Task<(int exitCode, bool completedOnItsOwn, string output, string error)> RunAgentAsync(
        string[] args,
        TimeSpan runFor)
    {
        using var cts = new CancellationTokenSource();
        var rootCommand = new RootCommand();
        rootCommand.Add(AgentCommand.Create(cts.Token));

        var output = new ThreadSafeStringWriter();
        var error = new ThreadSafeStringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;

        try
        {
            Console.SetOut(output);
            Console.SetError(error);

            var agentTask = Task.Run(async () => await rootCommand.Parse(args).InvokeAsync());

            // Wait until the agent reports that it is running (the banner precedes the released log
            // lines) or exits on its own, instead of assuming a fixed start-up time
            await NSerf.CLI.Tests.Helpers.TestHelper.WaitForConditionAsync(
                () => agentTask.IsCompleted || output.ToString().Contains("==> Log data will now stream in as it occurs:"),
                TimeSpan.FromSeconds(10));

            var completed = await Task.WhenAny(agentTask, Task.Delay(runFor)) == agentTask;

            cts.Cancel();
            var exitCode = await agentTask.WaitAsync(TimeSpan.FromSeconds(10));
            return (exitCode, completed, output.ToString(), error.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }
    }

    [Fact(Timeout = 20000)]
    public async Task AgentCommand_Discover_StartsMdnsAndKeepsRunning()
    {
        var (exitCode, completedOnItsOwn, output, error) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0",
            "--discover", "nserf-cli-discover-test"
        ], TimeSpan.FromSeconds(2));

        Assert.False(completedOnItsOwn, $"the agent must keep running with --discover (stderr: {error})");
        Assert.Equal(0, exitCode);
        Assert.Contains("nserf-cli-discover-test", output);
    }

    [Fact(Timeout = 20000)]
    public async Task AgentCommand_DefaultLogLevel_PrintsInfoLogs()
    {
        var (exitCode, _, output, _) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0"
        ], TimeSpan.FromSeconds(2));

        Assert.Equal(0, exitCode);
        Assert.Contains("[INFO]", output);
    }

    [Fact(Timeout = 20000)]
    public async Task AgentCommand_LogLevelWarn_SuppressesInfoLogs()
    {
        var (exitCode, _, output, _) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0",
            "--log-level", "WARN"
        ], TimeSpan.FromSeconds(2));

        Assert.Equal(0, exitCode);
        Assert.DoesNotContain("[INFO]", output);
    }

    [Fact(Timeout = 30000)]
    public async Task AgentCommand_RetryJoinExhausted_ExitsWithOne()
    {
        var (exitCode, completedOnItsOwn, output, _) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0",
            "--retry-join", "127.0.0.1:9",
            "--retry-interval", "100ms",
            "--retry-max", "2"
        ], TimeSpan.FromSeconds(15));

        Assert.True(completedOnItsOwn, "the agent exits once retry-join is exhausted");
        Assert.Equal(1, exitCode);
        Assert.Contains("maximum retry join attempts made, exiting", output);
    }

    private static AgentCommand.AgentOptions Options(string? configPath = null, string? retryInterval = null, string? logLevel = null) => new(
        NodeName: null,
        BindAddr: "0.0.0.0:7946",
        AdvertiseAddr: null,
        RpcAddr: "127.0.0.1:7373",
        RpcAuth: null,
        EncryptKey: null,
        JoinAddr: null,
        Replay: false,
        Tags: null,
        ConfigPath: configPath,
        HandlerSpecs: null,
        Discover: null,
        LogLevel: logLevel,
        RetryJoin: null,
        RetryInterval: retryInterval,
        RetryMax: null,
        SnapshotPath: null,
        Rejoin: false);

    private static async Task<string> WriteConfigAsync(string json)
    {
        var path = Path.Combine(Path.GetTempPath(), $"nserf-cli-config-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(path, json);
        return path;
    }

    /// <summary>
    /// Go: DefaultConfig &lt; config file &lt; CLI flags. A timeout set in the file must survive when the
    /// CLI does not set it (the CLI config must not carry defaults into the merge).
    /// </summary>
    [Fact]
    public async Task BuildConfig_FileValuesSurvive_WhenNoCliFlagOverridesThem()
    {
        var path = await WriteConfigAsync(
            "{ \"node_name\": \"from-file\", \"reconnect_interval\": \"90s\", \"reconnect_timeout\": \"1h\", " +
            "\"tombstone_timeout\": \"2h\", \"broadcast_timeout\": \"7s\", \"retry_interval\": \"45s\" }");
        try
        {
            var (config, _) = await AgentCommand.BuildConfigAsync(Options(configPath: path), CancellationToken.None);

            Assert.Equal("from-file", config.NodeName);
            Assert.Equal(TimeSpan.FromSeconds(90), config.ReconnectInterval);
            Assert.Equal(TimeSpan.FromHours(1), config.ReconnectTimeout);
            Assert.Equal(TimeSpan.FromHours(2), config.TombstoneTimeout);
            Assert.Equal(TimeSpan.FromSeconds(7), config.BroadcastTimeout);
            Assert.Equal(TimeSpan.FromSeconds(45), config.RetryInterval);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task BuildConfig_CliFlagOverridesFileValue_AndDefaultsFillTheRest()
    {
        var path = await WriteConfigAsync("{ \"retry_interval\": \"45s\" }");
        try
        {
            var (config, _) = await AgentCommand.BuildConfigAsync(Options(configPath: path, retryInterval: "10s"), CancellationToken.None);

            Assert.Equal(TimeSpan.FromSeconds(10), config.RetryInterval);
            Assert.Equal(TimeSpan.FromHours(72), config.ReconnectTimeout);
            Assert.Equal("127.0.0.1:7373", config.RpcAddr);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Go's flag parser uses time.ParseDuration, which rejects a bare number ("missing unit").</summary>
    [Fact(Timeout = 20000)]
    public async Task AgentCommand_BareNumberRetryInterval_IsRejected()
    {
        var (exitCode, completedOnItsOwn, _, error) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0",
            "--retry-join", "127.0.0.1:9",
            "--retry-interval", "30"
        ], TimeSpan.FromSeconds(5));

        Assert.True(completedOnItsOwn, "an invalid duration must fail the command");
        Assert.Equal(1, exitCode);
        Assert.Contains("Invalid duration for --retry-interval", error);
    }

    /// <summary>Go: setupLoggers prints "Invalid log level" and returns 1.</summary>
    [Fact(Timeout = 20000)]
    public async Task AgentCommand_InvalidLogLevel_ExitsWithOne()
    {
        var (exitCode, completedOnItsOwn, _, error) = await RunAgentAsync(
        [
            "agent",
            "--node", NextNodeName(),
            "--bind", "127.0.0.1:0",
            "--rpc-addr", "127.0.0.1:0",
            "--log-level", "bogus"
        ], TimeSpan.FromSeconds(5));

        Assert.True(completedOnItsOwn, "an invalid log level must fail the command");
        Assert.Equal(1, exitCode);
        Assert.Contains("Invalid log level: bogus", error);
    }

    [Fact(Timeout = 20000)]
    public async Task AgentCommand_SnapshotAndRejoin_AreAccepted()
    {
        var snapshotPath = Path.Combine(Path.GetTempPath(), $"nserf-cli-snapshot-{Guid.NewGuid():N}.snap");
        try
        {
            var (exitCode, completedOnItsOwn, _, error) = await RunAgentAsync(
            [
                "agent",
                "--node", NextNodeName(),
                "--bind", "127.0.0.1:0",
                "--rpc-addr", "127.0.0.1:0",
                "--snapshot", snapshotPath,
                "--rejoin"
            ], TimeSpan.FromSeconds(2));

            Assert.False(completedOnItsOwn, $"the agent must keep running with --snapshot/--rejoin (stderr: {error})");
            Assert.Equal(0, exitCode);
            Assert.True(File.Exists(snapshotPath), "the agent writes its snapshot to the --snapshot path");
        }
        finally
        {
            if (File.Exists(snapshotPath)) File.Delete(snapshotPath);
        }
    }
}
