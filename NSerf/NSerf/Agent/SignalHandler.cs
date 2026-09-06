// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Runtime.InteropServices;

namespace NSerf.Agent;

public enum Signal
{
    SIGINT,
    SIGTERM,
    SIGHUP
}

public delegate void SignalCallback(Signal signal);

/// <summary>
/// Cross-platform signal handling.
/// Maps to: Go's signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP) in command.go.
/// Unix: <see cref="PosixSignalRegistration"/> for SIGINT, SIGTERM and SIGHUP. The registrations cancel
/// the runtime's default handling so the process is not terminated before the callbacks run (which is
/// what makes a graceful leave on SIGTERM possible).
/// Windows: Console.CancelKeyPress for SIGINT and AppDomain.ProcessExit as the SIGTERM fallback.
/// </summary>
public sealed class SignalHandler : IDisposable
{
    private readonly List<SignalCallback> _callbacks = [];
    private readonly object _lock = new();
    private readonly List<PosixSignalRegistration> _registrations = [];
    private bool _disposed;

    public SignalHandler()
    {
        if (OperatingSystem.IsWindows())
        {
            // Register for Ctrl+C / Ctrl+Break
            Console.CancelKeyPress += OnCancelKeyPress;

            // Register for process exit (closest SIGTERM equivalent on Windows)
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
            return;
        }

        RegisterPosix(PosixSignal.SIGINT, Signal.SIGINT);
        RegisterPosix(PosixSignal.SIGTERM, Signal.SIGTERM);
        RegisterPosix(PosixSignal.SIGHUP, Signal.SIGHUP);
    }

    private void RegisterPosix(PosixSignal posixSignal, Signal signal)
    {
        try
        {
            _registrations.Add(PosixSignalRegistration.Create(posixSignal, context =>
            {
                // Keep the process alive: the callbacks decide when (and how) to exit.
                context.Cancel = true;
                TriggerSignal(signal);
            }));
        }
        catch (PlatformNotSupportedException)
        {
            // Signal not supported on this platform; nothing to register.
        }
    }

    public void RegisterCallback(SignalCallback callback)
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            _callbacks.Add(callback);
        }
    }

    public void TriggerSignal(Signal signal)
    {
        SignalCallback[] callbacks;
        lock (_lock)
        {
            if (_disposed)
                return;
            callbacks = [.. _callbacks];
        }

        foreach (var callback in callbacks)
        {
            try
            {
                callback(signal);
            }
            catch
            {
                // Ignore callback errors
            }
        }
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true; // Prevent immediate termination
        TriggerSignal(Signal.SIGINT);
    }

    private void OnProcessExit(object? sender, EventArgs e)
    {
        TriggerSignal(Signal.SIGTERM);
    }

    private void Dispose(bool disposing)
    {
        lock (_lock)
        {
            if (_disposed) return;
        }

        if (disposing)
        {
            // Free managed resources
            if (OperatingSystem.IsWindows())
            {
                Console.CancelKeyPress -= OnCancelKeyPress;
                AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;
            }

            foreach (var registration in _registrations)
            {
                registration.Dispose();
            }
            _registrations.Clear();
        }

        lock (_lock)
        {
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
    }
}
