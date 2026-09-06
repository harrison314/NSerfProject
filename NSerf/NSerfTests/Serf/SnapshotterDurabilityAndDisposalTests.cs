using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FluentAssertions;
using NSerf.Serf;
using NSerf.Serf.Events;
using Xunit;

namespace NSerfTests.Serf;

/// <summary>
/// Tests for issue #5: the snapshotter must not fsync on every member event
/// (Go only flushes its bufio writer periodically and syncs on compaction),
/// and shutdown/disposal must be deterministic and race-free.
/// </summary>
[Collection("Sequential Snapshot Tests")]
public class SnapshotterDurabilityAndDisposalTests : IDisposable
{
    private readonly List<string> _tempFiles = new();

    public void Dispose()
    {
        foreach (var file in _tempFiles)
        {
            try
            {
                if (File.Exists(file)) File.Delete(file);
                var compact = file + ".compact";
                if (File.Exists(compact)) File.Delete(compact);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    private string GetTempSnapshotPath()
    {
        var path = Path.Combine(Path.GetTempPath(), $"snapshotter_durability_test_{Guid.NewGuid()}.snapshot");
        _tempFiles.Add(path);
        return path;
    }

    private static MemberEvent JoinEvent(string name, int port) => new()
    {
        Type = EventType.MemberJoin,
        Members = new List<Member>
        {
            new()
            {
                Name = name,
                Addr = IPAddress.Parse("127.0.0.1"),
                Port = (ushort)port
            }
        }
    };

    /// <summary>
    /// N member events must not cause any fsync (Flush(flushToDisk: true)),
    /// while the "alive:" lines must still be visible to readers promptly
    /// (the StreamWriter buffer is flushed to the OS, not to disk).
    /// </summary>
    [Fact]
    public async Task MemberEvents_ShouldNotFsync_ButContentIsVisible()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource();

        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 1024 * 1024, false, null, clock, null, shutdownCts.Token);

        const int eventCount = 50;
        for (var i = 0; i < eventCount; i++)
        {
            await inCh.WriteAsync(JoinEvent($"node{i}", 8000 + i));
        }

        // Wait until all events have been recorded in memory
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (snap.AliveNodes().Count < eventCount && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }
        snap.AliveNodes().Should().HaveCount(eventCount, "all member events should be processed");

        // Content must be visible to readers without any fsync
        string content;
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (var reader = new StreamReader(fs))
        {
            content = await reader.ReadToEndAsync();
        }

        var aliveLines = content.Split('\n').Count(l => l.StartsWith("alive:"));
        aliveLines.Should().Be(eventCount, "every member event should be visible in the snapshot file");

        snap.SyncToDiskCount.Should().Be(0,
            "member events must not force an fsync per event (Go only syncs on compaction)");

        shutdownCts.Cancel();
        await snap.WaitAsync();
        await snap.DisposeAsync();
    }

    /// <summary>
    /// DisposeAsync must complete within a bounded time even when the external
    /// shutdown token is never cancelled (previously the clock loop only stopped
    /// on the external token, so DisposeAsync hung forever).
    /// </summary>
    [Fact]
    public async Task DisposeAsync_ShouldComplete_WithoutCancellingExternalToken()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource(); // deliberately never cancelled

        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 1024 * 1024, false, null, clock, null, shutdownCts.Token);

        await inCh.WriteAsync(JoinEvent("node1", 8001));
        await Task.Delay(100);

        var sw = Stopwatch.StartNew();
        var disposeTask = snap.DisposeAsync().AsTask();
        var completed = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(10)));
        sw.Stop();

        completed.Should().BeSameAs(disposeTask,
            "DisposeAsync must not depend on the external shutdown token being cancelled");
        await disposeTask; // surface any exception
        shutdownCts.IsCancellationRequested.Should().BeFalse("the test must not have cancelled the external token");

        // WaitAsync is completed at the end of StreamAsync, which DisposeAsync awaits
        snap.WaitAsync().IsCompleted.Should().BeTrue("the stream loop should have finished");

        var content = await File.ReadAllTextAsync(path);
        content.Should().Contain("alive: node1", "events processed before disposal must be persisted");
    }

    /// <summary>
    /// A synchronous Dispose() while events are still queued must stop the
    /// background tasks first and complete without throwing (no writer/file
    /// handle disposed underneath a running Stream task).
    /// </summary>
    [Fact]
    public async Task Dispose_WithQueuedEvents_ShouldNotThrow()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource();

        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 1024 * 1024, false, null, clock, null, shutdownCts.Token);

        // Queue a burst of events; many will still be pending when Dispose runs
        for (var i = 0; i < 2000; i++)
        {
            inCh.TryWrite(JoinEvent($"node{i % 100}", 8000 + (i % 100)));
        }

        var unobserved = 0;
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, args) =>
        {
            Interlocked.Increment(ref unobserved);
            args.SetObserved();
        };
        TaskScheduler.UnobservedTaskException += handler;
        try
        {
            var sw = Stopwatch.StartNew();
            var act = () => snap.Dispose();
            act.Should().NotThrow("Dispose must stop the background tasks before releasing the file");
            sw.Stop();
            sw.ElapsedMilliseconds.Should().BeLessThan(10000, "Dispose must be bounded");

            // Background tasks must have stopped (WaitAsync completes at the end of StreamAsync)
            var completed = await Task.WhenAny(snap.WaitAsync(), Task.Delay(TimeSpan.FromSeconds(5)));
            completed.Should().BeSameAs(snap.WaitAsync(), "the stream loop must terminate after Dispose");

            // Give any faulted background task a chance to be finalized/observed
            await Task.Delay(100);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            unobserved.Should().Be(0, "background tasks must tolerate a closed handle without unobserved exceptions");
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }

    /// <summary>
    /// Deterministic variant of SnapshotterUnitTest.Shutdown_ShouldCompleteWithinTimeout:
    /// with compaction disabled, draining a burst of queued member events at shutdown must
    /// be fast and must fsync at most once (the final shutdown sync), never per event.
    /// </summary>
    [Fact]
    public async Task ShutdownDrain_ShouldNotFsyncPerEvent_AndCompleteQuickly()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource();

        // Large minCompactSize so that compaction (which legitimately fsyncs) does not run
        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 64 * 1024 * 1024, false, null, clock, null, shutdownCts.Token);

        var joinEvent = JoinEvent("test1", 7946);
        for (var i = 0; i < 5000; i++)
        {
            inCh.TryWrite(joinEvent);
        }

        // Make sure at least one event reached the stream loop before shutting down, so the
        // persistence assertion below is about the shutdown drain rather than scheduling
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (snap.AliveNodes().Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
        snap.AliveNodes().Should().Contain(n => n.Name == "test1", "the first event should have been processed");

        shutdownCts.Cancel();
        var sw = Stopwatch.StartNew();
        await snap.WaitAsync();
        sw.Stop();

        sw.ElapsedMilliseconds.Should().BeLessThan(2000,
            "shutdown should complete quickly even with pending events");
        snap.SyncToDiskCount.Should().BeLessThanOrEqualTo(1,
            "draining queued member events must not fsync per event; only the final shutdown sync is allowed");

        var content = await File.ReadAllTextAsync(path);
        content.Should().Contain("alive: test1", "drained events must be persisted");

        await snap.DisposeAsync();
    }

    /// <summary>
    /// Production code does "await snap.WaitAsync(); snap.Dispose();" from thread-pool threads
    /// (no SynchronizationContext). The WaitAsync continuation must not run inline on the stream
    /// task's own thread, otherwise Dispose() waits (5 s) for the very task it is running inside.
    /// </summary>
    [Fact]
    public async Task Dispose_AfterWaitAsync_OnThreadPoolThread_ShouldNotStall()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource();

        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 1024 * 1024, false, null, clock, null, shutdownCts.Token);
        await inCh.WriteAsync(JoinEvent("node1", 8001));

        var elapsed = await Task.Run(async () =>
        {
            SynchronizationContext.SetSynchronizationContext(null);
            var waitTask = snap.WaitAsync();
            shutdownCts.Cancel();
            await waitTask.ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            snap.Dispose();
            sw.Stop();
            return sw.Elapsed;
        });

        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1),
            "Dispose() after WaitAsync() must not wait for the stream task its continuation was inlined into");
    }

    /// <summary>
    /// The leave marker must still be fsynced (durability of the leave is critical).
    /// </summary>
    [Fact]
    public async Task Leave_ShouldStillFsync()
    {
        var path = GetTempSnapshotPath();
        var clock = new LamportClock();
        var shutdownCts = new CancellationTokenSource();

        var (inCh, snap) = await Snapshotter.NewSnapshotterAsync(
            path, 1024 * 1024, false, null, clock, null, shutdownCts.Token);

        await inCh.WriteAsync(JoinEvent("node1", 8001));
        await Task.Delay(100);
        snap.SyncToDiskCount.Should().Be(0, "member events must not fsync");

        await snap.LeaveAsync();
        snap.SyncToDiskCount.Should().BeGreaterThanOrEqualTo(1, "the leave marker must be fsynced");

        var content = await File.ReadAllTextAsync(path);
        content.Should().Contain("leave", "the leave marker must be written");

        shutdownCts.Cancel();
        await snap.WaitAsync();
        await snap.DisposeAsync();
    }
}
