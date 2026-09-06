// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using Microsoft.Extensions.Logging;

namespace NSerf.Agent;

/// <summary>
/// ILogger decorator that copies every log line the agent (and the Serf/memberlist layers it hosts)
/// emits into the agent's <see cref="CircularLogWriter"/>, using Go's "[LEVEL] message" line format,
/// so the <c>monitor</c> RPC command can stream and level-filter them. Every call is also forwarded
/// to the user's logger. Maps to: Go's agent log writer (logutils.LevelFilter + logWriter).
/// </summary>
internal sealed class MonitorLogForwarder(ILogger? inner, CircularLogWriter writer, LogLevel minLevel) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner?.BeginScope(state);

    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel)
    {
        if (logLevel == Microsoft.Extensions.Logging.LogLevel.None) return false;
        return ToAgentLevel(logLevel) >= minLevel || (inner?.IsEnabled(logLevel) ?? false);
    }

    public void Log<TState>(
        Microsoft.Extensions.Logging.LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (logLevel == Microsoft.Extensions.Logging.LogLevel.None) return;

        var agentLevel = ToAgentLevel(logLevel);
        if (agentLevel >= minLevel)
        {
            var message = formatter(state, exception);
            if (exception != null)
            {
                message = $"{message} ({exception.GetType().Name}: {exception.Message})";
            }

            writer.WriteLine($"{agentLevel.ToPrefix()} {message}");
        }

        inner?.Log(logLevel, eventId, state, exception, formatter);
    }

    private static LogLevel ToAgentLevel(Microsoft.Extensions.Logging.LogLevel level) => level switch
    {
        Microsoft.Extensions.Logging.LogLevel.Trace => LogLevel.Trace,
        Microsoft.Extensions.Logging.LogLevel.Debug => LogLevel.Debug,
        Microsoft.Extensions.Logging.LogLevel.Information => LogLevel.Info,
        Microsoft.Extensions.Logging.LogLevel.Warning => LogLevel.Warn,
        _ => LogLevel.Error
    };
}
