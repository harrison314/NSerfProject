// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Diagnostics;
using System.Runtime.InteropServices;
using NSerf.Agent;

namespace NSerfTests.Agent;

/// <summary>
/// Verifies that <see cref="SignalHandler"/> registers real POSIX signals (Go: signal.Notify for
/// SIGINT, SIGTERM and SIGHUP) so the process is not terminated before the callbacks run.
/// </summary>
public class SignalHandlerPosixTests
{
    private const int SigHup = 1;
    private static readonly IntPtr SigIgn = 1;

#pragma warning disable SYSLIB1054 // a plain DllImport is enough for a test-only query of the signal disposition
    [DllImport("libc", EntryPoint = "sigaction")]
    private static extern int SigAction(int sig, IntPtr act, IntPtr oldact);
#pragma warning restore SYSLIB1054

    /// <summary>
    /// True when this process inherited SIG_IGN for <paramref name="sig"/> (nohup, or a background job of a
    /// non-interactive shell). The .NET runtime deliberately leaves ignored signals alone, so a
    /// PosixSignalRegistration never fires for them and the test cannot be meaningful in such a host.
    /// </summary>
    private static bool IsSignalIgnored(int sig)
    {
        var buffer = Marshal.AllocHGlobal(1024);
        try
        {
            for (var i = 0; i < 1024; i++) Marshal.WriteByte(buffer, i, 0);
            if (SigAction(sig, IntPtr.Zero, buffer) != 0) return false;

            // sa_handler / sa_sigaction is the first field of struct sigaction on macOS and Linux
            return Marshal.ReadIntPtr(buffer) == SigIgn;
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    [Fact]
    public async Task SignalHandler_RealSigHup_InvokesCallbackAndProcessSurvives()
    {
        if (OperatingSystem.IsWindows())
        {
            return; // POSIX signals only
        }

        if (IsSignalIgnored(SigHup))
        {
            Console.WriteLine("SIGHUP is ignored in this process (nohup / background job); real-signal delivery cannot be tested here");
            return;
        }

        using var handler = new SignalHandler();
        var received = new TaskCompletionSource<Signal>(TaskCreationOptions.RunContinuationsAsynchronously);
        handler.RegisterCallback(sig =>
        {
            if (sig == Signal.SIGHUP) received.TrySetResult(sig);
        });

        using var kill = Process.Start(new ProcessStartInfo("kill", $"-HUP {Environment.ProcessId}")
        {
            UseShellExecute = false,
            RedirectStandardError = true
        });
        kill.Should().NotBeNull();
        await kill!.WaitForExitAsync();
        kill.ExitCode.Should().Be(0, "kill must be able to deliver SIGHUP to the test process");

        var signal = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        signal.Should().Be(Signal.SIGHUP);
    }
}
