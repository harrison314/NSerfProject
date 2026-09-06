// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

namespace NSerf.Agent;

/// <summary>
/// Log levels for filtering log output.
/// Maps to: Go's logutils levels
/// </summary>
public enum LogLevel
{
    Trace,
    Debug,
    Info,
    Warn,
    Error
}

public static class LogLevelExtensions
{
    public static string ToPrefix(this LogLevel level)
    {
        return level switch
        {
            LogLevel.Trace => "[TRACE]",
            LogLevel.Debug => "[DEBUG]",
            LogLevel.Info => "[INFO]",
            LogLevel.Warn => "[WARN]",
            LogLevel.Error => "[ERR]",
            _ => "[INFO]"
        };
    }

    public static LogLevel FromString(string level)
    {
        return level.ToLower() switch
        {
            "trace" => LogLevel.Trace,
            "debug" => LogLevel.Debug,
            "info" => LogLevel.Info,
            "warn" or "warning" => LogLevel.Warn,
            "error" or "err" => LogLevel.Error,
            _ => LogLevel.Info
        };
    }

    /// <summary>
    /// Parses a log level name (TRACE, DEBUG, INFO, WARN/WARNING, ERR/ERROR; case-insensitive).
    /// Returns false for an unknown name (Go: "Invalid log level").
    /// </summary>
    public static bool TryFromString(string? level, out LogLevel result)
    {
        switch (level?.Trim().ToLowerInvariant())
        {
            case "trace": result = LogLevel.Trace; return true;
            case "debug": result = LogLevel.Debug; return true;
            case "info": result = LogLevel.Info; return true;
            case "warn" or "warning": result = LogLevel.Warn; return true;
            case "error" or "err": result = LogLevel.Error; return true;
            default: result = LogLevel.Info; return false;
        }
    }

    public static bool IsAtLeast(this LogLevel current, LogLevel minimum)
    {
        return current >= minimum;
    }
}
