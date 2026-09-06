// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using Microsoft.Extensions.Logging;

namespace NSerf.Agent;

/// <summary>
/// Main agent command with full lifecycle management: the single runner path used by the CLI and by
/// hosts that embed the agent. It creates a <see cref="SerfAgent"/> (which performs the start-join,
/// runs the retry-join loop and starts the RPC server), starts mDNS discovery, streams the agent's
/// log lines to the console through the level filter, and turns OS signals into Go's shutdown
/// semantics. Maps to: Go's cmd/serf/command/agent/command.go
/// </summary>
public class AgentCommand : IAsyncDisposable
{
    private readonly AgentConfig _config;
    private readonly ILogger? _logger;
    private readonly SignalHandler _signalHandler = new();
    private readonly TaskCompletionSource<int> _exitCodeTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _shutdownLock = new();
    private readonly object _stopLock = new();

    private SerfAgent? _agent;
    private AgentMdns? _mdns;
    private GatedWriter? _gatedWriter;
    private LogWriter? _logWriter;
    private TextWriter? _originalOut;
    private TextWriter? _installedOut;
    private ConsoleLogHandler? _consoleLogHandler;
    private Task? _stopTask;
    private int _signalCount;
    private bool _started;
    private bool _disposed;
    // Set by every forced exit: the agent is then shut down without a leave (Go: agent.Shutdown()),
    // so the cluster detects the node as failed instead of seeing a graceful leave.
    private volatile bool _skipLeave;

    private const int GracefulTimeoutSeconds = 3;

    public AgentCommand(AgentConfig config, ILogger? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger;

        // Setup signal handling
        _signalHandler.RegisterCallback(HandleSignal);
    }

    /// <summary>
    /// Invoked on SIGHUP with the running agent (Go: handleReload). The CLI uses it to re-read its
    /// configuration file and call <see cref="SerfAgent.UpdateEventHandlers"/>. Exceptions are logged.
    /// </summary>
    public Func<SerfAgent, CancellationToken, Task>? ReloadHandler { get; set; }

    /// <summary>
    /// The agent created by <see cref="RunAsync"/>, or null before the runner has started.
    /// </summary>
    public SerfAgent? Agent => _agent;

    /// <summary>
    /// Delivers a signal to the runner as if the OS had raised it (same semantics as a real
    /// SIGINT/SIGTERM/SIGHUP). Useful for hosts that own the shutdown decision.
    /// </summary>
    public void SendSignal(Signal signal) => _signalHandler.TriggerSignal(signal);

    public async Task<int> RunAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_started)
            throw new InvalidOperationException("Agent command already started");
        _started = true;

        SetupLogOutput();

        try
        {
            WriteStatus("==> Starting Serf agent...");

            // Create and start the agent: this performs the start-join, starts the retry-join loop
            // and the RPC server (Go: setupAgent + startupJoin + retryJoin + startupRPC).
            _agent = new SerfAgent(_config, _logger);
            _consoleLogHandler = new ConsoleLogHandler(_logWriter!);
            _agent.LogWriter?.RegisterHandler(_consoleLogHandler);
            _agent.RetryJoinExhausted += OnRetryJoinExhausted;

            await _agent.StartAsync(cancellationToken);

            StartMdns();

            WriteStatus("==> Serf agent running!");
            WriteStatus($"         Node name: '{_config.NodeName}'");
            WriteStatus($"         Bind addr: '{_config.BindAddr}'");
            WriteStatus($"          RPC addr: '{_agent.RpcAddress ?? _config.RpcAddr ?? string.Empty}'");
            WriteStatus($"         Encrypted: {!string.IsNullOrEmpty(_config.EncryptKey) || !string.IsNullOrEmpty(_config.KeyringFile)}");
            WriteStatus($"          Snapshot: {!string.IsNullOrEmpty(_config.SnapshotPath)}");
            WriteStatus($"           Profile: {_config.Profile}");
            if (!string.IsNullOrEmpty(_config.Discover))
            {
                WriteStatus($"     mDNS cluster: '{_config.Discover}'");
            }
            WriteStatus(string.Empty);
            WriteStatus("==> Log data will now stream in as it occurs:");
            WriteStatus(string.Empty);

            // Release buffered logs now that startup succeeded
            _gatedWriter!.Flush();

            // Wait for a signal-driven exit, retry-join exhaustion, or cancellation by the host
            try
            {
                return await _exitCodeTcs.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Host cancellation: shut down gracefully below
                return _exitCodeTcs.Task.IsCompletedSuccessfully ? _exitCodeTcs.Task.Result : 0;
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[Agent] Failed to start agent");
            WriteStatus($"Error starting agent: {ex.Message}", error: true);
            return 1;
        }
        finally
        {
            // Go: defer agent.Shutdown()
            await StopAgentAsync();
            RestoreConsole();

            // The runner no longer owns the process signals once it has returned
            _signalHandler.Dispose();
        }
    }

    private void SetupLogOutput()
    {
        // Logs are gated until startup succeeded, then released to the console at the configured level
        _originalOut = Console.Out;
        _gatedWriter = new GatedWriter(_originalOut);
        var level = string.IsNullOrEmpty(_config.LogLevel) ? LogLevel.Info : LogLevelExtensions.FromString(_config.LogLevel);
        _logWriter = new LogWriter(_gatedWriter, level);

        // Route direct console writes through the level filter as well
        Console.SetOut(_logWriter);
        _installedOut = Console.Out;
    }

    private void RestoreConsole()
    {
        lock (_stopLock)
        {
            if (_originalOut == null) return;

            // Only put the original writer back if ours is still installed (a host may have redirected since)
            if (ReferenceEquals(Console.Out, _installedOut))
            {
                Console.SetOut(_originalOut);
            }

            _originalOut = null;
            _installedOut = null;
        }

        _gatedWriter?.Flush();
    }

    private void StartMdns()
    {
        if (string.IsNullOrEmpty(_config.Discover) || _agent?.Serf?.Memberlist == null)
            return;

        var localNode = _agent.Serf.Memberlist.LocalNode;
        var bindAddr = localNode.Addr;
        var bindPort = localNode.Port;

        // Get mDNS interface (use mdns-specific interface if set, otherwise fall back to main interface)
        var interfaceName = _config.Mdns.Interface ?? _config.Interface;
        if (!string.IsNullOrEmpty(interfaceName))
        {
            var mdnsInterface = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
                .FirstOrDefault(i => i.Name.Equals(interfaceName, StringComparison.OrdinalIgnoreCase));

            if (mdnsInterface == null)
            {
                LogLine(LogLevel.Warn, $"mDNS interface '{interfaceName}' not found, using default");
            }
        }

        LogLine(LogLevel.Info, $"Starting mDNS listener for cluster: {_config.Discover}");

        _mdns = new AgentMdns(
            _agent,
            replay: _config.ReplayOnJoin,
            node: _config.NodeName,
            discover: _config.Discover,
            bind: bindAddr,
            port: bindPort,
            disableIPv4: _config.Mdns.DisableIPv4,
            disableIPv6: _config.Mdns.DisableIPv6,
            logger: _logger);
    }

    private void OnRetryJoinExhausted(int attempts)
    {
        // Go: "[ERR] agent: maximum retry join attempts made, exiting" -> exit code 1, no leave
        LogLine(LogLevel.Error, "maximum retry join attempts made, exiting");
        ForceShutdown();
    }

    private void HandleSignal(Signal signal)
    {
        lock (_shutdownLock)
        {
            WriteStatus($"Caught signal: {signal}");

            // SIGHUP triggers a config reload and does not count towards the shutdown signals
            if (signal == Signal.SIGHUP)
            {
                _ = Task.Run(ReloadConfigAsync);
                return;
            }

            _signalCount++;

            // First signal: graceful shutdown (if configured)
            if (_signalCount == 1)
            {
                var graceful = signal == Signal.SIGINT && !_config.SkipLeaveOnInt || signal == Signal.SIGTERM && _config.LeaveOnTerm;

                if (graceful)
                {
                    WriteStatus("Gracefully shutting down agent...");
                    _ = Task.Run(GracefulShutdownAsync);
                }
                else
                {
                    ForceShutdown();
                }
            }
            // Second signal: force shutdown
            else
            {
                LogLine(LogLevel.Warn, "Force shutdown due to second signal");
                ForceShutdown();
            }
        }
    }

    private async Task GracefulShutdownAsync()
    {
        try
        {
            var serf = _agent?.Serf;
            if (serf != null)
            {
                var leaveTask = serf.LeaveAsync();
                var completed = await Task.WhenAny(leaveTask, Task.Delay(TimeSpan.FromSeconds(GracefulTimeoutSeconds)));
                if (completed != leaveTask)
                {
                    LogLine(LogLevel.Error, $"Timeout ({GracefulTimeoutSeconds}s) while waiting for graceful leave, forcing shutdown");

                    // The abandoned leave may still fail later; observe it so it never surfaces as unobserved
                    _ = leaveTask.ContinueWith(t => _ = t.Exception, CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                    ForceShutdown();
                    return;
                }

                await leaveTask;
            }

            Complete(0);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[Agent] Error during graceful shutdown");
            LogLine(LogLevel.Error, $"Error while leaving: {ex.Message}");
            ForceShutdown();
        }
    }

    private void ForceShutdown()
    {
        _skipLeave = true;
        Complete(1);
    }

    private void Complete(int exitCode) => _exitCodeTcs.TrySetResult(exitCode);

    private async Task ReloadConfigAsync()
    {
        try
        {
            LogLine(LogLevel.Info, "Reloading configuration...");

            var agent = _agent;
            var handler = ReloadHandler;
            if (agent != null && handler != null)
            {
                await handler(agent, CancellationToken.None);
            }

            LogLine(LogLevel.Info, "Reload completed");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[Agent] Failed to reload config");
            LogLine(LogLevel.Error, $"Reload failed: {ex.Message}");
        }
    }

    private Task StopAgentAsync()
    {
        lock (_stopLock)
        {
            _stopTask ??= StopAgentCoreAsync();
            return _stopTask;
        }
    }

    private async Task StopAgentCoreAsync()
    {
        // Dispose mDNS first to stop discovery
        _mdns?.Dispose();
        _mdns = null;

        var agent = _agent;
        if (agent == null) return;

        agent.RetryJoinExhausted -= OnRetryJoinExhausted;
        try
        {
            // A forced exit stops the agent without leaving (Go: agent.Shutdown()); every other exit
            // (graceful signal, host cancellation) leaves the cluster first.
            await agent.ShutdownAsync(leave: !_skipLeave);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[Agent] Error during shutdown");
            LogLine(LogLevel.Error, $"Error during shutdown: {ex.Message}");
        }

        if (_consoleLogHandler != null)
        {
            agent.LogWriter?.DeregisterHandler(_consoleLogHandler);
        }

        await agent.DisposeAsync();
    }

    /// <summary>
    /// Writes a Go-style "[LEVEL] agent: message" line. The line goes through the agent's log writer
    /// (so monitor clients see it too) and therefore to the console via the level filter.
    /// </summary>
    private void LogLine(LogLevel level, string message)
    {
        var line = $"{level.ToPrefix()} agent: {message}";
        var agentWriter = _agent?.LogWriter;
        if (agentWriter != null)
        {
            agentWriter.WriteLine(line);
        }
        else
        {
            _logWriter?.WriteLine(line);
        }

        _logger?.Log(ToLoggerLevel(level), "[Agent] {Message}", message);
    }

    /// <summary>
    /// Writes a status line straight to the console (Go: c.Ui.Output / c.Ui.Error), bypassing the log gate.
    /// </summary>
    private void WriteStatus(string message, bool error = false)
    {
        try
        {
            if (error)
            {
                Console.Error.WriteLine(message);
                return;
            }

            (_originalOut ?? Console.Out).WriteLine(message);
        }
        catch (Exception)
        {
            // Console may be unavailable (e.g. closed stream); status output is best-effort
        }
    }

    private static Microsoft.Extensions.Logging.LogLevel ToLoggerLevel(LogLevel level) => level switch
    {
        LogLevel.Trace => Microsoft.Extensions.Logging.LogLevel.Trace,
        LogLevel.Debug => Microsoft.Extensions.Logging.LogLevel.Debug,
        LogLevel.Info => Microsoft.Extensions.Logging.LogLevel.Information,
        LogLevel.Warn => Microsoft.Extensions.Logging.LogLevel.Warning,
        _ => Microsoft.Extensions.Logging.LogLevel.Error
    };

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Unblock RunAsync if it is still waiting
        Complete(0);

        await StopAgentAsync();
        RestoreConsole();

        _signalHandler.Dispose();

        if (_gatedWriter != null)
        {
            await _gatedWriter.DisposeAsync();
        }
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Forwards the agent's circular log buffer to the console log writer (level filtered).
    /// </summary>
    private sealed class ConsoleLogHandler(TextWriter writer) : CircularLogWriter.ILogHandler
    {
        public void HandleLog(string log) => writer.WriteLine(log);
    }
}
