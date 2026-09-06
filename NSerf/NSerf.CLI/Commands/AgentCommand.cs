// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using NSerf.Agent;

namespace NSerf.CLI.Commands;

/// <summary>
/// Agent command - starts a long-running Serf agent.
/// This is the core command that all other commands depend on. It builds the <see cref="AgentConfig"/>
/// from the defaults, the config file(s) and the CLI flags (in that precedence order) and then hands the
/// lifecycle to the library runner (<see cref="NSerf.Agent.AgentCommand"/>): joins, retry-join, RPC,
/// mDNS discovery, log streaming and signal handling.
/// Port of: serf/cmd/serf/command/agent/command.go
/// </summary>
public static class AgentCommand
{
    private const string DefaultBindAddr = "0.0.0.0:7946";
    private const string DefaultRpcAddr = "127.0.0.1:7373";

    public static Command Create(CancellationToken shutdownToken = default)
    {
        var command = new Command("agent", "Start the Serf agent");

        var nodeOption = new Option<string?>("--node")
        {
            Description = "Node name (default: hostname)"
        };

        var bindOption = new Option<string>("--bind")
        {
            Description = $"Bind address (default: {DefaultBindAddr})",
            DefaultValueFactory = _ => DefaultBindAddr
        };

        var advertiseOption = new Option<string?>("--advertise")
        {
            Description = "Advertise address"
        };

        var rpcAddrOption = new Option<string>("--rpc-addr")
        {
            Description = $"RPC bind address (default: {DefaultRpcAddr})",
            DefaultValueFactory = _ => DefaultRpcAddr
        };

        var rpcAuthOption = new Option<string?>("--rpc-auth")
        {
            Description = "RPC authentication token"
        };

        var encryptOption = new Option<string?>("--encrypt")
        {
            Description = "Encryption key (16-byte base64)"
        };

        var joinOption = new Option<string?>("--join")
        {
            Description = "Address to join at startup"
        };

        var replayOption = new Option<bool>("--replay")
        {
            Description = "Replay user events on join"
        };

        var tagOption = new Option<string[]?>("--tag")
        {
            Description = "Node tag (key=value)",
            AllowMultipleArgumentsPerToken = true
        };

        var configFileOption = new Option<string?>("--config-file")
        {
            Description = "Path to a config file or directory of .json files (loaded before CLI overrides)"
        };

        var eventHandlerOption = new Option<string[]?>("--event-handler")
        {
            Description = "Event handler specification (can be repeated)",
            AllowMultipleArgumentsPerToken = true
        };

        var discoverOption = new Option<string?>("--discover")
        {
            Description = "Cluster name for mDNS discovery: advertise and join peers of the same cluster"
        };

        var logLevelOption = new Option<string?>("--log-level")
        {
            Description = "Log level of the agent output (TRACE, DEBUG, INFO, WARN, ERR; default: INFO)"
        };

        var retryJoinOption = new Option<string[]?>("--retry-join")
        {
            Description = "Address to join with retries at startup (can be repeated)",
            AllowMultipleArgumentsPerToken = true
        };

        var retryIntervalOption = new Option<string?>("--retry-interval")
        {
            Description = "Time to wait between join attempts, as a Go duration (e.g. 30s; default: 30s)"
        };

        var retryMaxOption = new Option<int?>("--retry-max")
        {
            Description = "Maximum join attempts before exiting with an error (default: 0 = retry forever)"
        };

        var snapshotOption = new Option<string?>("--snapshot")
        {
            Description = "Path to a snapshot file used to restore state after a restart"
        };

        var rejoinOption = new Option<bool>("--rejoin")
        {
            Description = "Rejoin the cluster on restart even after a graceful leave"
        };

        command.Add(nodeOption);
        command.Add(bindOption);
        command.Add(advertiseOption);
        command.Add(rpcAddrOption);
        command.Add(rpcAuthOption);
        command.Add(encryptOption);
        command.Add(joinOption);
        command.Add(replayOption);
        command.Add(tagOption);
        command.Add(configFileOption);
        command.Add(eventHandlerOption);
        command.Add(discoverOption);
        command.Add(logLevelOption);
        command.Add(retryJoinOption);
        command.Add(retryIntervalOption);
        command.Add(retryMaxOption);
        command.Add(snapshotOption);
        command.Add(rejoinOption);

        command.SetAction(async (parseResult, _) =>
        {
            var options = new AgentOptions(
                NodeName: parseResult.GetValue(nodeOption),
                BindAddr: parseResult.GetValue(bindOption)!,
                AdvertiseAddr: parseResult.GetValue(advertiseOption),
                RpcAddr: parseResult.GetValue(rpcAddrOption)!,
                RpcAuth: parseResult.GetValue(rpcAuthOption),
                EncryptKey: parseResult.GetValue(encryptOption),
                JoinAddr: parseResult.GetValue(joinOption),
                Replay: parseResult.GetValue(replayOption),
                Tags: parseResult.GetValue(tagOption),
                ConfigPath: parseResult.GetValue(configFileOption),
                HandlerSpecs: parseResult.GetValue(eventHandlerOption),
                Discover: parseResult.GetValue(discoverOption),
                LogLevel: parseResult.GetValue(logLevelOption),
                RetryJoin: parseResult.GetValue(retryJoinOption),
                RetryInterval: parseResult.GetValue(retryIntervalOption),
                RetryMax: parseResult.GetValue(retryMaxOption),
                SnapshotPath: parseResult.GetValue(snapshotOption),
                Rejoin: parseResult.GetValue(rejoinOption));

            try
            {
                return await ExecuteAsync(options, shutdownToken);
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync($"Error: {ex.Message}");
                return 1;
            }
        });

        return command;
    }

    internal sealed record AgentOptions(
        string? NodeName,
        string BindAddr,
        string? AdvertiseAddr,
        string RpcAddr,
        string? RpcAuth,
        string? EncryptKey,
        string? JoinAddr,
        bool Replay,
        string[]? Tags,
        string? ConfigPath,
        string[]? HandlerSpecs,
        string? Discover,
        string? LogLevel,
        string[]? RetryJoin,
        string? RetryInterval,
        int? RetryMax,
        string? SnapshotPath,
        bool Rejoin);

    private static async Task<int> ExecuteAsync(AgentOptions options, CancellationToken shutdownToken)
    {
        // Go: setupLoggers rejects an unknown log level before the agent starts
        if (!string.IsNullOrWhiteSpace(options.LogLevel) && !LogLevelExtensions.TryFromString(options.LogLevel, out _))
        {
            await Console.Error.WriteLineAsync($"Invalid log level: {options.LogLevel}. Valid log levels are: TRACE, DEBUG, INFO, WARN, ERR");
            return 1;
        }

        var (finalConfig, cliConfig) = await BuildConfigAsync(options, shutdownToken);

        // Hand the lifecycle to the library runner; SIGHUP re-reads the config file and reloads the event handlers
        await using var runner = new NSerf.Agent.AgentCommand(finalConfig)
        {
            ReloadHandler = async (agent, token) =>
            {
                // Re-read the config file (if any) with the CLI flags applied on top, then reload the
                // handlers. Failures propagate to the runner, which logs them.
                var reloaded = cliConfig;
                if (!string.IsNullOrWhiteSpace(options.ConfigPath))
                {
                    var loaded = await LoadConfigAsync(options.ConfigPath, token);
                    reloaded = AgentConfig.Merge(loaded, cliConfig);
                }

                agent.UpdateEventHandlers(reloaded.EventHandlers);
                Console.WriteLine($"Reloaded {reloaded.EventHandlers.Count} event handler(s)");
            }
        };

        return await runner.RunAsync(shutdownToken);
    }

    /// <summary>
    /// Builds the effective configuration the way Go does: DefaultConfig, then the config file(s), then
    /// the CLI flags, later sources overriding earlier ones. Every value the CLI did not set is left at
    /// its zero value in the returned CLI config so the merge never overrides a file value with a default.
    /// </summary>
    internal static async Task<(AgentConfig Config, AgentConfig CliConfig)> BuildConfigAsync(AgentOptions options, CancellationToken cancellationToken)
    {
        // Build CLI config - only contains values explicitly set via CLI flags
        // This matches Go's behavior where cmdConfig only has non-zero values
        var cliConfig = new AgentConfig(replayOnJoin: options.Replay)
        {
            // Set to empty/zero so merge logic knows these weren't provided
            BindAddr = string.Empty,
            NodeName = string.Empty,
            LogLevel = string.Empty,
            Profile = string.Empty,
            SyslogFacility = string.Empty,
            RetryInterval = TimeSpan.Zero,
            RetryIntervalWan = TimeSpan.Zero,
            ReconnectInterval = TimeSpan.Zero,
            ReconnectTimeout = TimeSpan.Zero,
            TombstoneTimeout = TimeSpan.Zero,
            BroadcastTimeout = TimeSpan.Zero,
            Protocol = 0,
            UserEventSizeLimit = 0
        };

        // Only set values that were explicitly provided via CLI flags
        if (!string.IsNullOrWhiteSpace(options.NodeName))
            cliConfig.NodeName = options.NodeName;
        if (!string.IsNullOrWhiteSpace(options.BindAddr) && options.BindAddr != DefaultBindAddr)
            cliConfig.BindAddr = options.BindAddr;
        if (!string.IsNullOrWhiteSpace(options.AdvertiseAddr))
            cliConfig.AdvertiseAddr = options.AdvertiseAddr;
        if (!string.IsNullOrWhiteSpace(options.EncryptKey))
            cliConfig.EncryptKey = options.EncryptKey;
        if (!string.IsNullOrWhiteSpace(options.RpcAddr) && options.RpcAddr != DefaultRpcAddr)
            cliConfig.RpcAddr = options.RpcAddr;
        if (!string.IsNullOrWhiteSpace(options.RpcAuth))
            cliConfig.RpcAuthKey = options.RpcAuth;
        if (options.Tags is { Length: > 0 })
            cliConfig.Tags = ParseTags(options.Tags);
        if (options.HandlerSpecs is { Length: > 0 })
            cliConfig.EventHandlers = [.. options.HandlerSpecs];
        if (!string.IsNullOrWhiteSpace(options.Discover))
            cliConfig.Discover = options.Discover;
        if (!string.IsNullOrWhiteSpace(options.LogLevel))
            cliConfig.LogLevel = options.LogLevel;
        if (options.RetryJoin is { Length: > 0 })
            cliConfig.RetryJoin = [.. options.RetryJoin];
        if (!string.IsNullOrWhiteSpace(options.RetryInterval))
            cliConfig.RetryInterval = ParseDuration(options.RetryInterval, "--retry-interval");
        if (options.RetryMax is > 0)
            cliConfig.RetryMaxAttempts = options.RetryMax.Value;
        if (!string.IsNullOrWhiteSpace(options.SnapshotPath))
            cliConfig.SnapshotPath = options.SnapshotPath;
        if (options.Rejoin)
            cliConfig.RejoinAfterLeave = true;

        // --join is a start-join: SerfAgent joins after start and fails startup (exit 1) when nobody could be joined
        if (!string.IsNullOrWhiteSpace(options.JoinAddr))
            cliConfig.StartJoin = [options.JoinAddr];

        // CRITICAL: Match Go's config loading order:
        // 1. Start with defaults
        // 2. Merge file config into defaults
        // 3. Merge CLI config into result
        var finalConfig = AgentConfig.Default();

        if (!string.IsNullOrWhiteSpace(options.ConfigPath))
        {
            var loaded = await LoadConfigAsync(options.ConfigPath, cancellationToken);

            // Merge file config into defaults
            finalConfig = AgentConfig.Merge(finalConfig, loaded);
        }

        // Merge CLI config into result (CLI overrides file and defaults)
        finalConfig = AgentConfig.Merge(finalConfig, cliConfig);

        // Apply final defaults for required fields (Go: the RPC server always listens, on 127.0.0.1:7373 by default)
        if (string.IsNullOrWhiteSpace(finalConfig.NodeName))
            finalConfig.NodeName = Environment.MachineName;
        if (string.IsNullOrWhiteSpace(finalConfig.RpcAddr))
            finalConfig.RpcAddr = options.RpcAddr;

        return (finalConfig, cliConfig);
    }

    private static async Task<AgentConfig> LoadConfigAsync(string configPath, CancellationToken cancellationToken)
    {
        if (Directory.Exists(configPath))
            return await ConfigLoader.LoadFromDirectoryAsync(configPath, cancellationToken);

        if (File.Exists(configPath))
            return await ConfigLoader.LoadFromFileAsync(configPath, cancellationToken);

        throw new FileNotFoundException($"Config file or directory not found: {configPath}");
    }

    private static TimeSpan ParseDuration(string value, string optionName)
    {
        // Go durations only (time.ParseDuration): a bare number has no unit and is rejected
        try
        {
            return GoDurationJsonConverter.ParseDuration(value);
        }
        catch (FormatException)
        {
            throw new FormatException($"Invalid duration for {optionName}: '{value}' (expected a Go duration such as 30s, 500ms or 1m)");
        }
    }

    private static Dictionary<string, string> ParseTags(string[]? tags)
    {
        var result = new Dictionary<string, string>();

        if (tags == null)
            return result;

        foreach (var tag in tags)
        {
            var parts = tag.Split('=', 2);
            if (parts.Length == 2)
            {
                result[parts[0]] = parts[1];
            }
        }

        return result;
    }
}
