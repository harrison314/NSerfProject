// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0
// Ported from: github.com/hashicorp/serf/serf/snapshot.go

using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NSerf.Metrics;
using NSerf.Serf.Events;

namespace NSerf.Serf;

/// <summary>
/// Serf supports using a "snapshot" file that contains various
/// transactional data that is used to help Serf recover quickly
/// and gracefully from a failure. We append member events, as well
/// as the latest clock values to the file during normal operation,
/// and periodically checkpoint and roll over the file. During a restore,
/// we can replay the various member events to recall a list of known
/// nodes to re-join, as well as restore our clock values to avoid replaying
/// old events.
/// </summary>
public class Snapshotter : IDisposable, IAsyncDisposable
{
    private const int FlushIntervalMs = 500;
    private const int ClockUpdateIntervalMs = 500;
    private const string TmpExt = ".compact";
    private const int SnapshotErrorRecoveryIntervalMs = 30000;
    private const int EventChSize = 2048;
    private const int ShutdownFlushTimeoutMs = 250;
    private const int SnapshotBytesPerNode = 128;
    private const int SnapshotCompactionThreshold = 2;

    private readonly Dictionary<string, string> _aliveNodes = [];
    private readonly LamportClock _clock;
    private FileStream? _fileHandle;
    private StreamWriter? _bufferedWriter;
    private readonly ChannelReader<IEvent> _inCh;
    private readonly Channel<IEvent> _streamCh;
    private DateTime _lastFlush = DateTime.UtcNow;
    private readonly Channel<bool> _leaveCh = Channel.CreateUnbounded<bool>();
    private bool _leaving;
    private readonly ILogger? _logger;
    private readonly long _minCompactSize;
    private readonly string _path;
    private long _offset;
    private readonly ChannelWriter<IEvent>? _outCh;
    private readonly bool _rejoinAfterLeave;
    private readonly CancellationToken _shutdownToken;
    // Internal stop signal: linked to the external shutdown token, and also cancelled by
    // DisposeAsync/Dispose so disposal never depends on the external token being cancelled.
    private readonly CancellationTokenSource _stopCts;
    private readonly CancellationToken _stopToken;
    private volatile bool _disposed;
    private const int DisposeWaitTimeoutMs = 5000;
    // RunContinuationsAsynchronously: code that does "await WaitAsync(); Dispose();" must not run
    // inline on the stream task's own thread, otherwise Dispose() would wait on itself.
    private readonly TaskCompletionSource _waitTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private DateTime _lastAttemptedCompaction = DateTime.MinValue;
    private readonly object _lock = new();
    private Task? _teeTask;
    private Task? _streamTask;
    private readonly object _fileLock = new();
    // Serialises event processing and clock updates (Go runs both in the stream goroutine), so the
    // append/compaction path is never entered by the clock loop and the stream loop at the same time.
    private readonly object _processLock = new();
    private readonly IMetrics? _metrics;
    private readonly MetricLabel[]? _metricLabels;
    private int _syncToDiskCount;

    /// <summary>
    /// Number of times the snapshot has been fsynced (Flush(flushToDisk: true)).
    /// Instrumentation only; used by tests to assert durability cost.
    /// </summary>
    internal int SyncToDiskCount => Volatile.Read(ref _syncToDiskCount);

    /// <summary>
    /// The single place that forces data to the physical disk (fsync).
    /// </summary>
    private void SyncToDisk(FileStream? fs)
    {
        if (fs is null) return;
        Interlocked.Increment(ref _syncToDiskCount);
        fs.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Creates a new Snapshotter that records events up to a
    /// max byte size before rotating the file. It can also be used to
    /// recover old state. Snapshotter works by reading an event channel it returns,
    /// passing through to an output channel, and persisting relevant events to disk.
    /// Setting rejoinAfterLeave makes leave not clear the state and can be used
    /// if you intend to rejoin the same cluster after a leave.
    /// </summary>
    public static async Task<(ChannelWriter<IEvent> InCh, Snapshotter Snap)> NewSnapshotterAsync(
        string path,
        int minCompactSize,
        bool rejoinAfterLeave,
        ILogger? logger,
        LamportClock clock,
        ChannelWriter<IEvent>? outCh,
        CancellationToken shutdownToken)
    {
        var inCh = Channel.CreateBounded<IEvent>(new BoundedChannelOptions(EventChSize)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait  // Apply backpressure when full
        });

        // Try to open the file with retry logic to handle file locks from previous instances
        FileStream fh;
        var retries = 5;
        while (true)
        {
            try
            {
                fh = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                break;
            }
            catch (IOException ex) when (retries > 0)
            {
                retries--;
                logger?.LogWarning(ex, "Failed to open snapshot (retries left: {Retries}): {Error}", retries, ex.Message);
                await Task.Delay(100, shutdownToken);
            }
            catch (Exception ex)
            {
                throw new IOException($"Failed to open snapshot: {ex.Message}", ex);
            }
        }

        // Determine the offset
        var offset = fh.Length;

        // Create the snapshotter
        var snap = new Snapshotter(
            aliveNodes: [],
            clock: clock,
            fileHandle: fh,
            inCh: inCh.Reader,
            lastClock: new LamportTime(0),
            lastEventClock: new LamportTime(0),
            lastQueryClock: new LamportTime(0),
            logger: logger,
            minCompactSize: minCompactSize,
            path: path,
            offset: offset,
            outCh: outCh,
            rejoinAfterLeave: rejoinAfterLeave,
            shutdownToken: shutdownToken,
            metrics: null,
            metricLabels: null);

        // Recover the last known state
        try
        {
            await snap.ReplayAsync();
        }
        catch (Exception)
        {
            fh.Close();
            throw;
        }

        // Start handling new commands
        snap.StartProcessing();

        return (inCh.Writer, snap);
    }

    private Snapshotter(
        Dictionary<string, string> aliveNodes,
        LamportClock clock,
        FileStream fileHandle,
        ChannelReader<IEvent> inCh,
        LamportTime lastClock,
        LamportTime lastEventClock,
        LamportTime lastQueryClock,
        ILogger? logger,
        long minCompactSize,
        string path,
        long offset,
        ChannelWriter<IEvent>? outCh,
        bool rejoinAfterLeave,
        CancellationToken shutdownToken,
        IMetrics? metrics,
        MetricLabel[]? metricLabels)
    {
        _aliveNodes = aliveNodes;
        _clock = clock;
        _fileHandle = fileHandle;
        _bufferedWriter = new StreamWriter(fileHandle, Encoding.UTF8, bufferSize: 4096, leaveOpen: true);
        _inCh = inCh;
        _streamCh = Channel.CreateBounded<IEvent>(new BoundedChannelOptions(EventChSize)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait  // Apply backpressure when full
        });
        LastClock = lastClock;
        LastEventClock = lastEventClock;
        LastQueryClock = lastQueryClock;
        _logger = logger;
        _minCompactSize = minCompactSize;
        _path = path;
        _offset = offset;
        _outCh = outCh;
        _rejoinAfterLeave = rejoinAfterLeave;
        _shutdownToken = shutdownToken;
        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
        _stopToken = _stopCts.Token;
        _metrics = metrics;
        _metricLabels = metricLabels;
    }

    /// <summary>
    /// Returns the last known clock time
    /// </summary>
    public LamportTime LastClock { get; private set; }

    /// <summary>
    /// Returns the last known event clock time
    /// </summary>
    public LamportTime LastEventClock { get; private set; }

    /// <summary>
    /// Returns the last known query clock time
    /// </summary>
    public LamportTime LastQueryClock { get; private set; }

    /// <summary>
    /// Returns the last known alive nodes (in randomized order to prevent hot shards)
    /// </summary>
    public List<PreviousNode> AliveNodes()
    {
        lock (_lock)
        {
            var previous = _aliveNodes.Select(kvp => new PreviousNode(kvp.Key, kvp.Value)).ToList();

            // Randomize the order (Fisher-Yates shuffle)
            var rng = new Random();
            for (var i = previous.Count - 1; i > 0; i--)
            {
                var j = rng.Next(i + 1);
                (previous[i], previous[j]) = (previous[j], previous[i]);
            }

            return previous;
        }
    }

    /// <summary>
    /// Wait is used to wait until the snapshotter finishes shut down
    /// </summary>
    public Task WaitAsync() => _waitTcs.Task;

    /// <summary>
    /// Leave is used to remove known nodes to prevent a restart from
    /// causing a join. Otherwise, nodes will re-join after leaving!
    /// </summary>
    public Task LeaveAsync()
    {
        // Process leave immediately and synchronously to ensure it's written before shutdown.
        // HandleLeave appends the marker and flushes/fsyncs under the file lock; the StreamWriter
        // is not thread-safe, so no unsynchronised pre-flush is performed here.
        lock (_processLock)
        {
            HandleLeave();
        }

        return Task.CompletedTask;
    }

    private void StartProcessing()
    {
        _logger?.LogDebug("[Snapshotter/StartProcessing] Starting TeeStream and Stream tasks...");

        _teeTask = Task.Factory.StartNew(
            async () => await TeeStreamAsync(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

        _streamTask = Task.Factory.StartNew(
            async () => await StreamAsync(),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

        _logger?.LogDebug("[Snapshotter/StartProcessing] Tasks started successfully");
    }

    /// <summary>
    /// teeStream is a long-running routine used to copy events
    /// to the output channel and the internal event handler.
    /// </summary>
    private async Task TeeStreamAsync()
    {
        _logger?.LogDebug("[Snapshotter/TeeStream] Task started, waiting for events...");
        try
        {
            // Use ReadAllAsync to automatically respect cancellation
            await foreach (var evt in _inCh.ReadAllAsync(_stopToken))
            {
                _logger?.LogDebug("[Snapshotter/TeeStream] Received event: {Type}", evt.GetType().Name);

                // Forward to a stream channel (may block on backpressure)
                try
                {
                    await _streamCh.Writer.WriteAsync(evt, _stopToken);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown requested - stop forwarding
                    break;
                }
                catch (ChannelClosedException)
                {
                    // Stream channel completed by disposal - stop forwarding
                    break;
                }

                // Forward to the output channel if configured (non-blocking)
                if (_outCh == null) continue;
                try
                {
                    await _outCh.WriteAsync(evt, _stopToken);
                }
                catch (OperationCanceledException)
                {
                    // Continue - outCh failure shouldn't stop snapshot writes
                }
                catch (ChannelClosedException)
                {
                    // Continue - outCh failure shouldn't stop snapshot writes
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger?.LogDebug("[Snapshotter/TeeStream] Task cancelled (shutdown)");
        }
        finally
        {
            // Critical: Signal that no more events will be written to streamCh
            // This allows StreamAsync to drain all remaining events safely
            try
            {
                _streamCh.Writer.Complete();
                _logger?.LogDebug("[Snapshotter/TeeStream] Completed streamCh writer");
            }
            catch
            {
                // Ignore completion errors
            }
        }
    }

    /// <summary>
    /// stream is a long-running routine that is used to handle events
    /// </summary>
    private async Task StreamAsync()
    {
        _logger?.LogDebug("[Snapshotter/Stream] Task started, processing events...");

        // The clock-update loop must stop when the stream loop ends, regardless of
        // whether the external shutdown token was ever cancelled (e.g. DisposeAsync).
        using var clockCts = CancellationTokenSource.CreateLinkedTokenSource(_stopToken);
        var clockToken = clockCts.Token;
        var clockTask = Task.Run(async () =>
        {
            using var clockTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(ClockUpdateIntervalMs));
            while (!clockToken.IsCancellationRequested)
            {
                try
                {
                    await clockTimer.WaitForNextTickAsync(clockToken);
                    lock (_processLock)
                    {
                        UpdateClock();
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogDebug(ex, "[Snapshotter] Clock update error");
                }
            }
        }, CancellationToken.None);

        try
        {
            await foreach (var evt in _streamCh.Reader.ReadAllAsync(_stopToken))
            {
                ProcessStreamItem(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Stream processing error");
        }
        finally
        {
            clockCts.Cancel();
            await clockTask;
        }

        try
        {
            await PerformShutdownFlushAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Shutdown flush error");
        }

        try
        {
            lock (_fileLock)
            {
                _fileHandle?.Close();
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "[Snapshotter] Error closing snapshot file");
        }

        _waitTcs.TrySetResult();
    }

    /// <summary>
    /// Handles one item of the stream loop: a pending leave first, then the event itself.
    /// </summary>
    private void ProcessStreamItem(IEvent evt)
    {
        lock (_processLock)
        {
            // Check for leave
            if (_leaveCh.Reader.TryRead(out _))
            {
                HandleLeave();
            }

            // Process the event
            FlushEvent(evt);
        }
    }

    private async Task PerformShutdownFlushAsync()
    {
        lock (_processLock)
        {
            // Snapshot the clock first
            UpdateClock();

            // Process any pending leave events FIRST
            while (_leaveCh.Reader.TryRead(out _))
            {
                HandleLeave();
            }
        }

        // Drain the events that are already queued, bounded by the flush timeout (Go's FLUSH loop
        // selects on flushTimeout for every event). ReadAllAsync only observes cancellation while it
        // waits for an item, so the deadline is also checked once per event.
        using var cts = new CancellationTokenSource(ShutdownFlushTimeoutMs);
        var drainTimedOut = false;
        try
        {
            await foreach (var evt in _streamCh.Reader.ReadAllAsync(cts.Token))
            {
                if (cts.IsCancellationRequested)
                {
                    drainTimedOut = true;
                    break;
                }

                ProcessStreamItem(evt);
            }
        }
        catch (OperationCanceledException)
        {
            drainTimedOut = true;
        }

        if (drainTimedOut)
        {
            // Timeout reached - acceptable data loss scenario
            _logger?.LogWarning("[Snapshotter] Shutdown drain timeout - some pending events may not be persisted");
        }

        // Final flush: flush the buffer and fsync once (Go: s.buffered.Flush(); s.fh.Sync())
        try
        {
            lock (_fileLock)
            {
                if (_disposed) return;
                _bufferedWriter?.Flush();
                SyncToDisk(_fileHandle);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to flush snapshot during shutdown");
        }
    }

    private void HandleLeave()
    {
        _leaving = true;

        // If we plan to re-join, keep our state
        if (!_rejoinAfterLeave)
        {
            lock (_lock)
            {
                _aliveNodes.Clear();
            }

            // Only write the leave marker when NOT rejoining
            // When rejoin-after-leave is enabled, a snapshot is used to rejoin known peers
            TryAppend("leave\n");
        }

        // Ensure the leave marker is flushed to disk before returning (critical for durability)
        try
        {
            lock (_fileLock)
            {
                if (!_disposed)
                {
                    // Flush writer buffer
                    _bufferedWriter?.Flush();

                    // Force fsync to ensure data reaches the disk (critical for leave marker)
                    SyncToDisk(_fileHandle);
                }
            }

            _logger?.LogDebug("[Snapshotter] Leave marker successfully written and flushed");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to flush leave to snapshot");
        }
    }

    private void FlushEvent(IEvent e)
    {
        // Stop recording events after a leave is issued
        if (_leaving)
        {
            return;
        }

        switch (e)
        {
            case MemberEvent memberEvent:
                ProcessMemberEvent(memberEvent);
                break;
            case UserEvent userEvent:
                ProcessUserEvent(userEvent);
                break;
            case Query query:
                ProcessQuery(query);
                break;
            default:
                _logger?.LogError("Unknown event to snapshot: {EventType}", e.GetType().Name);
                break;
        }
    }

    private void ProcessMemberEvent(MemberEvent e)
    {
        _logger?.LogDebug("[Snapshotter] Processing MemberEvent: {Type} with {Count} members",
            e.EventType(), e.Members.Count);

        lock (_lock)
        {
            switch (e.EventType())
            {
                case EventType.MemberJoin:
                    foreach (var mem in e.Members)
                    {
                        var addr = $"{mem.Addr}:{mem.Port}";
                        _aliveNodes[mem.Name] = addr;
                        _logger?.LogDebug("[Snapshotter] Recording alive node: {Name} at {Addr}", mem.Name, addr);
                        TryAppend($"alive: {mem.Name} {addr}\n");
                    }
                    break;

                case EventType.MemberLeave:
                case EventType.MemberFailed:
                    foreach (var name in e.Members.Select(mem => mem.Name))
                    {
                        _aliveNodes.Remove(name);
                        _logger?.LogDebug("[Snapshotter] Recording not-alive node: {Name}", name);
                        TryAppend($"not-alive: {name}\n");
                    }
                    break;
            }

            _logger?.LogDebug("[Snapshotter] Total alive nodes in memory: {Count}", _aliveNodes.Count);
        }
        UpdateClock();
        // Flush the writer buffer to the OS so the snapshot is visible promptly.
        // No fsync here: Go only flushes its bufio writer periodically and syncs on compaction;
        // an fsync per member event costs milliseconds and makes shutdown drains take seconds.
        FlushBufferToOs();
    }

    /// <summary>
    /// Flushes the StreamWriter buffer through to the OS page cache (no fsync).
    /// </summary>
    private void FlushBufferToOs()
    {
        try
        {
            lock (_fileLock)
            {
                if (_disposed) return;
                _bufferedWriter?.Flush();
                _lastFlush = DateTime.UtcNow;
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Snapshot flush error");
        }
    }

    private void UpdateClock()
    {
        // Don't write clock updates after leave
        if (_leaving) return;

        var now = _clock.Time();
        if (now == 0) return;  // Guard against underflow

        var lastSeen = now - 1;
        if (lastSeen <= LastClock) return;
        LastClock = lastSeen;
        TryAppend($"clock: {(ulong)LastClock}\n");
    }

    private void ProcessUserEvent(UserEvent e)
    {
        // Ignore old clocks
        if (e.LTime <= LastEventClock)
        {
            return;
        }

        LastEventClock = e.LTime;
        TryAppend($"event-clock: {(ulong)e.LTime}\n");
        // Make the clock visible promptly (OS flush, no fsync)
        FlushBufferToOs();
    }

    private void ProcessQuery(Query q)
    {
        // Ignore old clocks
        if (q.LTime <= LastQueryClock)
        {
            return;
        }

        LastQueryClock = q.LTime;
        TryAppend($"query-clock: {(ulong)q.LTime}\n");
        // Make the clock visible promptly (OS flush, no fsync)
        FlushBufferToOs();
    }

    private void TryAppend(string line)
    {
        try
        {
            AppendLine(line);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to update snapshot");

            var now = DateTime.UtcNow;
            if ((now - _lastAttemptedCompaction).TotalMilliseconds > SnapshotErrorRecoveryIntervalMs)
            {
                _lastAttemptedCompaction = now;
                _logger?.LogInformation("Attempting compaction to recover from error...");

                try
                {
                    Compact();
                    _logger?.LogInformation("Finished compaction, successfully recovered from error state");
                }
                catch (Exception compactEx)
                {
                    _logger?.LogError(compactEx, "Compaction failed, will reattempt after {Interval}ms", SnapshotErrorRecoveryIntervalMs);
                }
            }
        }
    }

    private void AppendLine(string line)
    {
        // Emit duration metric (Go snapshot.go:397)
        // Reference: defer metrics.MeasureSinceWithLabels([]string{"serf", "snapshot", "appendLine"}, time.Now(), s.metricLabels)
        using (_metrics?.MeasureSince(["serf", "snapshot", "appendLine"], _metricLabels))
        {
            var bytes = Encoding.UTF8.GetByteCount(line);

            lock (_fileLock)
            {
                // Tolerate a closed handle: after disposal there is nothing to append to
                if (_disposed || _bufferedWriter is null) return;

                _bufferedWriter.Write(line);

                // Check if we should flush
                var now = DateTime.UtcNow;
                if ((now - _lastFlush).TotalMilliseconds > FlushIntervalMs)
                {
                    _lastFlush = now;
                    _bufferedWriter.Flush();
                }

                _offset += bytes;
            }

            // Check compaction outside the file lock to avoid long-held locks during I/O
            if (!_disposed && _offset > SnapshotMaxSize())
            {
                Compact();
            }
        }
    }

    private long SnapshotMaxSize()
    {
        lock (_lock)
        {
            long nodes = _aliveNodes.Count;
            var estSize = nodes * SnapshotBytesPerNode;
            var threshold = estSize * SnapshotCompactionThreshold;

            // Apply a minimum threshold
            if (threshold < _minCompactSize)
            {
                threshold = _minCompactSize;
            }
            return threshold;
        }
    }

    private void Compact()
    {
        // Emit duration metric (Go snapshot.go:436)
        // Reference: defer metrics.MeasureSinceWithLabels([]string{"serf", "snapshot", "compact"}, time.Now(), s.metricLabels)
        using (_metrics?.MeasureSince(["serf", "snapshot", "compact"], _metricLabels))
        {
            if (_disposed) return;

            var newPath = _path + TmpExt;

            // Step 1: Snapshot the alive nodes (SHORT lock)
            Dictionary<string, string> aliveSnapshot;
            lock (_lock)
            {
                aliveSnapshot = new Dictionary<string, string>(_aliveNodes);
            }

            // Write to a new file (NO lock - this is the slow part)
            long newOffset;
            using (var newFile = new FileStream(newPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(newFile, Encoding.UTF8))
            {
                newOffset = 0;

                // Write out alive nodes (no lock needed - we have a snapshot)
                foreach (var (name, addr) in aliveSnapshot)
                {
                    var line = $"alive: {name} {addr}\n";
                    writer.Write(line);
                    newOffset += Encoding.UTF8.GetByteCount(line);
                }

                // Write out clocks as raw integers
                var clockLine = $"clock: {(ulong)LastClock}\n";
                writer.Write(clockLine);
                newOffset += Encoding.UTF8.GetByteCount(clockLine);

                var eventClockLine = $"event-clock: {(ulong)LastEventClock}\n";
                writer.Write(eventClockLine);
                newOffset += Encoding.UTF8.GetByteCount(eventClockLine);

                var queryClockLine = $"query-clock: {(ulong)LastQueryClock}\n";
                writer.Write(queryClockLine);
                newOffset += Encoding.UTF8.GetByteCount(queryClockLine);

                writer.Flush();
                SyncToDisk(newFile);
            } // Close and flush a new file before swap

            // Atomic file swap (SHORT lock - just the swap operation)
            lock (_fileLock)
            {
                if (_disposed)
                {
                    try { File.Delete(newPath); }
                    catch
                    {
                        // ignored
                    }
                    return;
                }

                try
                {
                    _bufferedWriter?.Flush();
                    _bufferedWriter?.Dispose();
                    _fileHandle?.Close();
                }
                catch { /* ignore */ }

                // Replace an old file with a new file
                try { File.Delete(_path); }
                catch
                {
                    // ignored
                }

                File.Move(newPath, _path);

                // Reopen
                _fileHandle = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                _bufferedWriter = new StreamWriter(_fileHandle, Encoding.UTF8, bufferSize: 4096, leaveOpen: true);
                _offset = newOffset;
                _lastFlush = DateTime.UtcNow;

                // Don't re-append clocks - they're already in the compacted file
                // CRITICAL: Preserve leave marker if we've left and not rejoining
                if (_leaving && !_rejoinAfterLeave)
                {
                    _bufferedWriter.Write("leave\n");
                    _offset += Encoding.UTF8.GetByteCount("leave\n");
                }

                _bufferedWriter.Flush();
                SyncToDisk(_fileHandle);  // Force fsync for durability
            }
        }
    }
    /// <summary>
    /// Replay is used to reset our internal state by replaying
    /// the snapshot file. It is used at initialization time to read old state.
    /// </summary>
    private async Task ReplayAsync()
    {
        // Seek to the beginning
        _fileHandle!.Seek(0, SeekOrigin.Begin);

        // Read each line
        using var reader = new StreamReader(_fileHandle, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);

        while (await reader.ReadLineAsync(_shutdownToken) is { } line)
        {
            if (line.StartsWith("alive: "))
            {
                var info = line["alive: ".Length..];
                var lastSpaceIdx = info.LastIndexOf(' ');
                if (lastSpaceIdx == -1)
                {
                    _logger?.LogWarning("Failed to parse address: {Line}", line);
                    continue;
                }
                var addr = info[(lastSpaceIdx + 1)..];
                var name = info[..lastSpaceIdx];
                lock (_lock)
                {
                    _aliveNodes[name] = addr;
                }
            }
            else if (line.StartsWith("not-alive: "))
            {
                var name = line["not-alive: ".Length..];
                lock (_lock)
                {
                    _aliveNodes.Remove(name);
                }
            }
            else if (line.StartsWith("clock: "))
            {
                var timeStr = line["clock: ".Length..];
                if (ulong.TryParse(timeStr, out var time))
                {
                    LastClock = new LamportTime(time);
                }
                else
                {
                    _logger?.LogWarning("Failed to parse clock time: {TimeStr}", timeStr);
                }
            }
            else if (line.StartsWith("event-clock: "))
            {
                var timeStr = line["event-clock: ".Length..];
                if (ulong.TryParse(timeStr, out var time))
                {
                    LastEventClock = new LamportTime(time);
                }
                else
                {
                    _logger?.LogWarning("Failed to parse event clock time: {TimeStr}", timeStr);
                }
            }
            else if (line.StartsWith("query-clock: "))
            {
                var timeStr = line["query-clock: ".Length..];
                if (ulong.TryParse(timeStr, out var time))
                {
                    LastQueryClock = new LamportTime(time);
                }
                else
                {
                    _logger?.LogWarning("Failed to parse query clock time: {TimeStr}", timeStr);
                }
            }
            else if (line.StartsWith("coordinate: "))
            {
                // Ignore old coordinates
                continue;
            }
            else if (line == "leave")
            {
                // Ignore leave if we plan on re-joining
                if (_rejoinAfterLeave)
                {
                    _logger?.LogInformation("Ignoring previous leave in snapshot");
                    continue;
                }
                lock (_lock)
                {
                    _aliveNodes.Clear();
                }
                LastClock = new LamportTime(0);
                LastEventClock = new LamportTime(0);
                LastQueryClock = new LamportTime(0);
                // Stop processing rest of file after leave marker
                break;
            }
            else if (line.StartsWith('#'))
            {
                // Skip comment lines
            }
            else if (!string.IsNullOrWhiteSpace(line))
            {
                _logger?.LogWarning("Unrecognized snapshot line: {Line}", line);
            }
        }

        // Seek to the end for appending
        _fileHandle.Seek(0, SeekOrigin.End);
    }

    /// <summary>
    /// Disposes the snapshotter asynchronously, waiting for background tasks to complete.
    /// This is the preferred disposal method as it ensures all pending writes are flushed.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Stop the background tasks. This does not require the external shutdown
        // token to be cancelled: the internal stop token ends the tee/stream loops
        // (and therefore the clock loop), and the stream loop performs its final flush.
        SignalStop();

        // Wait for background tasks to finish
        var tasks = BackgroundTasks();
        if (tasks.Count > 0)
        {
            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error waiting for snapshotter tasks to complete");
            }
        }

        // Dispose of resources synchronously
        Dispose();

        // Suppress finalization to align with a recommended dispose pattern.
        // This ensures that derived types with finalizers don't need to re-implement
        // IDisposable/IAsyncDisposable solely to call SuppressFinalize.
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Synchronous disposal. Use DisposeAsync() when possible.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // Stop the background tasks first (bounded wait) so the writer/file handle are
        // not disposed underneath the Tee/Stream tasks.
        SignalStop();
        var tasks = BackgroundTasks();
        if (tasks.Count > 0)
        {
            try
            {
                // The continuation observes any fault (reads Exception) so a timed-out wait never throws
                var completed = Task.WhenAll(tasks)
                    .ContinueWith(t => { _ = t.Exception; }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
                    .Wait(DisposeWaitTimeoutMs);
                if (!completed)
                {
                    _logger?.LogWarning("[Snapshotter] Background tasks did not stop within {Timeout}ms; disposing anyway", DisposeWaitTimeoutMs);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug(ex, "[Snapshotter] Error waiting for background tasks during Dispose");
            }
        }

        lock (_fileLock)
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;

            try
            {
                _bufferedWriter?.Dispose();
            }
            catch (Exception)
            {
                // Already disposed by StreamAsync / underlying stream closed
            }

            try
            {
                _fileHandle?.Dispose();
            }
            catch (Exception)
            {
                // Already disposed by StreamAsync
            }
        }

        try
        {
            _stopCts.Dispose();
        }
        catch (Exception)
        {
            // ignored
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Requests the background tasks to stop without cancelling the external shutdown token.
    /// </summary>
    private void SignalStop()
    {
        try
        {
            if (!_stopCts.IsCancellationRequested)
            {
                _stopCts.Cancel();
            }
        }
        catch (ObjectDisposedException)
        {
            // Already disposed
        }
        catch (AggregateException ex)
        {
            _logger?.LogDebug(ex, "[Snapshotter] Error signalling stop");
        }

        // Signal no more events will be written to streamCh (TeeStream does this too)
        _streamCh.Writer.TryComplete();
    }

    private List<Task> BackgroundTasks()
    {
        var tasks = new List<Task>(2);
        if (_teeTask != null) tasks.Add(_teeTask);
        if (_streamTask != null) tasks.Add(_streamTask);
        return tasks;
    }
}

/// <summary>
/// PreviousNode is used to represent the previously known alive nodes
/// </summary>
public record PreviousNode(string Name, string Addr)
{
    public override string ToString() => $"{Name}: {Addr}";
}
