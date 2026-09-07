# NSerf code review: required changes

Scope: every file of the `NSerf` library (184 files, 27,260 lines), the `NSerf.CLI` project (19 files, 2,021 lines), the two test projects (50,382 lines, sampled for structure), the four example projects (skimmed for API usage), plus a full-strength Roslyn analyzer pass (`AnalysisLevel=latest-all`, `AnalysisMode=All`, `EnforceCodeStyleInBuild`) on the library and the CLI.

The project is a faithful port of HashiCorp Serf and memberlist and it works, but the code still reads like Go written in C#: Go-style accessors and error values, channels in the public API, `defer`-style lock helpers, partial classes standing in for Go files, hand-rolled framing and parsing, and diary comments referencing Go line numbers. The list below is ordered by what matters most. Items marked **Defect** are behavioural problems found while reading; the rest are design, idiom and hygiene changes. Line numbers refer to the current `dev` branch.

Analyzer baseline (unique warnings, library): 1,620. Largest buckets: `CA1848` logging performance (459), `CA2007` missing `ConfigureAwait` (453), `CA1873` logging argument evaluation (175), `CA1031` catching `Exception` (159), `CA1062` unvalidated public arguments (90), `CA1819` array-typed properties (50), `CA1002` `List<T>` in signatures (33), `CA2227` settable collection properties (27). CLI: 134, dominated by `CA2007` (96) and `CA1515` (19 public types that should be internal). No `.editorconfig` exists, so no naming or style rule is enforced today.

---

## 1. Correctness defects found during the review (fix first)

1. **Defect: the alive delegate cannot reject nodes.** `IAliveDelegate.NotifyAlive` returns an error string, but `StateHandlers.HandleAliveNode` ignores the return value and only treats a thrown exception as rejection (`Memberlist/State/StateHandlers.cs:76`). Either honour the returned string or change the contract to a `bool`/exception (see 2.3).
2. **Defect: `MemberReap` events bypass the event pipeline.** `BackgroundTasks.EraseNode` writes the reap event straight to `Config.EventCh` (`Serf/BackgroundTasks.cs:204-213`). Go replaces `conf.EventCh` with the snapshotter's input channel, so its direct write still flows through snapshotter and query handler. NSerf keeps `Config.EventCh` as the user's channel and routes everything else through `EmitEvent`, so reap events skip the snapshotter, the internal query handler, and the IPC channel that `NSerfServiceProvider` reads. Route it through `EmitEvent`.
3. **Defect: event coalescing is configured but never wired.** `Config.CoalescePeriod`, `QuiescentPeriod`, `UserCoalescePeriod` and `UserQuiescentPeriod` exist, `Serf/Coalesce/*` implements the loop, but nothing calls `CoalesceLoop.CoalescedEventChannel`. Wire it in `Serf.CreateAsync` like Go's `coalescedEventCh`, or remove the four options and the folder.
4. **Defect: broadcast queue depth is unbounded.** `Config.MaxQueueDepth`, `MinQueueDepth` and `RecentIntentTimeout` are never read. Go's `checkQueueDepth` prunes the event and query queues to `MaxQueueDepth` (or `max(MinQueueDepth, 2*NumNodes)`); `HandleQueueMonitorAsync` (`Serf/BackgroundTasks.cs:93-125`) only logs a warning. Implement the prune, or remove the options.
5. **Defect: service discovery reads a lossy channel.** `Serf.IpcEventReader` is a bounded channel of 64 with `DropWrite` and is documented for "multiple IPC clients", but it is a single-consumer channel (two readers would each see only some events) and its only consumer is `NSerfServiceProvider` (`ServiceDiscovery/NSerfServiceProvider.cs:162`). Under a burst of member events the provider silently misses registrations. Replace with a proper subscription (see 2.4).
6. **Defect: TCP fallback pings are never answered.** `Memberlist.Network.cs:628-650` (`ProcessInnerMessageAsync`) handles only `PushPull` and `User` on a stream; Go's `handleConn` also handles `pingMsg`. An NSerf peer logs "Unknown stream message type: Ping", so the fallback added for indirect probing cannot succeed against real transports. The client side (`SendPingAndWaitForAckAsync`, lines 857-928) also reads the reply with a single 1,024-byte read and ignores the length-prefix framing it expects everywhere else.
7. **Defect: graceful leave ignores the broadcast timeout.** `Memberlist/LeaveManager.cs` forces three gossip rounds with fixed 200 ms sleeps plus a fixed 500 ms sleep, returns the timeout in `LeaveResult` without using it, and never waits on the dead broadcast's notify channel (Go: `broadcastTimeout` select). Every leave costs about one second regardless of cluster size, and a slow cluster is not given the configured time.
8. **Defect: user-controlled regular expressions without timeouts.** `Serf.ShouldProcessQuery` builds a `Regex` from tag filters received from any cluster peer (`Serf/Query.cs:332`) with no `RegexOptions` and no match timeout, so a peer can pin a CPU with a pathological pattern. `RpcSession.HandleMembersFilteredAsync` does the same for RPC input (`Agent/RPC/RpcSession.cs:422-432`). Add a timeout (`TimeSpan.FromMilliseconds(100)`), cache compiled filters, and reject invalid patterns once.
9. **Defect: unobserved fire-and-forget network sends.** `PacketHandler.HandlePing` discards `memberlist.SendPacketAsync(...)` (`Memberlist/Handlers/PacketHandler.cs:272`) and `Events/Query.cs:186` discards relay sends. A failing send becomes an `UnobservedTaskException` instead of a log line. Every discarded task must go through a helper that logs faults.
10. **Defect: non-thread-safe `Random` for query IDs.** `Serf._queryRandom` (`Serf/Serf.cs:43`) is a shared `System.Random` used from `QueryAsync` without a lock; concurrent queries corrupt its state. Query IDs are also only 31 bits from `Next()`. Use `Random.Shared.NextInt64` or `RandomNumberGenerator.GetInt32`.
11. **Defect: sync-over-async inside a gossip callback.** `MergeDelegate.NotifyMerge` calls `_serf.Config.Merge?.NotifyMerge(...).GetAwaiter().GetResult()` (`Serf/MergeDelegate.cs:45`) on the user's asynchronous merge delegate from the push/pull handler; on a starved thread pool this deadlocks the join. Make the memberlist merge path asynchronous end to end or make the Serf delegate synchronous.
12. **Defect: conflicting defaults.** `Config.UserEventSizeLimit` is `9 * 1024` in the property initializer and `512` in `Config.DefaultConfig()` (`Serf/Config.cs:202,353`); `MemberlistConfig` repeats every default in `DefaultLANConfig()`. Keep one source of truth (initializers) and delete the duplicated factory bodies.
13. **Defect: `RpcEventHandler` and `RpcLogHandler` do blocking socket writes on the agent's event and log threads** (`Agent/RPC/RpcEventHandler.cs:82-95`, `RpcLogHandler.cs:63-76`). A slow or stalled RPC client stalls every event handler of the agent and every log line. Go uses buffered per-stream goroutines. Queue records per stream into a bounded channel drained by one writer task, and drop with a log when the client falls behind.
14. **Defect: `QueryResponseStream` polls with `Task.Delay(10)`** (`Agent/RPC/QueryResponseStream.cs:83`) instead of awaiting the two channels; `AgentMdns.RunAsync` and `CoalesceLoop` do the same with `Task.Delay(100)` and `Task.WhenAny` over `Task.Delay(Infinite, token)` tasks allocated per iteration.
15. **Defect: `AgentMdns` uses an `async void` timer callback** (`Agent/AgentMdns.cs:145`) and a process-global `Lazy<MulticastService>` that is never disposed but whose `ServiceDiscovery` wrapper is disposed per instance (`AgentMdns.cs:22,308`).
16. **Defect: the `Profile` setting is never applied.** `AgentConfig.Profile` ("lan", "wan", "local") is accepted, merged and printed, but `SerfAgent.BuildConfig` always starts from `DefaultLANConfig()` (`Agent/SerfAgent.cs:249-251`); Go selects `DefaultLANConfig`/`DefaultWANConfig`/`DefaultLocalConfig`. Apply it or remove it.
17. **Defect: `NSerfServiceProviderOptions.AutoMarkFailedUnhealthy` and `AutoDeregisterOnLeave` are never read**; `ServiceDiscoveryOptions` (health checks, TTL, export debounce) and `IServiceExporter` have no implementation anywhere in the solution. Either implement or delete them so the public surface does not promise behaviour that does not exist.
18. **Defect: `Keyring.Pkcs7Decode` does not validate padding bytes** (`Memberlist/Security/SecurityTools.cs:176-190`); a malformed version-0 payload can produce a negative length and throw `OverflowException`/`ArgumentException` from a decrypt path that callers treat as "try next key". Validate `1 <= padding <= 16` and every pad byte.

---

## 2. Public API shape (breaking changes; batch them into one major release)

### 2.1 Names that collide with their namespace or with the BCL

`CA1724` flags four; several more are confusing in practice:

| Current | Problem | Proposed |
| --- | --- | --- |
| `NSerf.Serf.Serf` | type = namespace; every consumer writes `NSerf.Serf.Serf` | `SerfNode` (or rename the namespace to `NSerf.Cluster`) |
| `NSerf.Memberlist.Memberlist` | type = namespace | `MemberlistNode` / namespace `NSerf.Gossip` |
| `NSerf.Coordinate.Coordinate` | type = namespace; `Coordinate.Coordinate` throughout `Serf.cs` | `NetworkCoordinate` |
| `NSerf.Serf.Delegate` | hides `System.Delegate` | `SerfGossipDelegate` |
| `NSerf.Serf.Config` | generic; siblings are `MemberlistConfig`, `AgentConfig`, `NetTransportConfig`, `CoordinateConfig` | `SerfConfig` |
| `NSerf.Agent.LogLevel` | collides with `Microsoft.Extensions.Logging.LogLevel`; `MonitorLogForwarder` has to fully qualify both | `AgentLogLevel` or reuse the M.E.L enum |
| `NSerf.ServiceDiscovery.IServiceProvider` | collides with `System.IServiceProvider`, which every DI consumer already has in scope | `IServiceDiscoveryProvider` |
| `NSerf.Client.Responses.Member` vs `NSerf.Serf.Member` | same name, different shapes | `RpcMember` |
| `NSerf.Serf.MessageType` vs `NSerf.Memberlist.Messages.MessageType` | both imported in `Delegate.cs`, `Query.cs`, `Events/Query.cs` | `SerfMessageType` / `GossipMessageType` |
| `SerfAgent.Serf` property of type `NSerf.Serf.Serf` | `agent.Serf.Serf`-style confusion | `Node`/`Cluster` |
| `SerfState.SerfAlive` etc. | enum members repeat the type name (Go style) | `SerfState.Alive`, `Leaving`, `Left`, `Shutdown` |

### 2.2 Go accessor methods that should be properties

`NumMembers()`, `Members()`, `State()`, `LocalMember()`, `ProtocolVersion()`, `EncryptionEnabled()`, `IsReady()`, `GetTags()`, `Stats()`, `DefaultQueryTimeout()`, `DefaultQueryParams()` on `Serf`; `NumMembers()`, `Members()`, `GetHealthScore()` on `Memberlist`; `Time()` on `LamportClock`; `GetHealthScore()` on `Awareness`; `GetCoordinate()`/`Stats()` on `CoordinateClient`; `SourceNode()`/`GetDeadline()` on `Events.Query`; `EventType()` on `IEvent`; `Finished()` on `QueryResponse`; `Message()`/`Name()` on `IBroadcast`/`INamedBroadcast`; `DeadOrLeft()`/`Address()` on `NodeState`; `String()` extension on `EventType` (`Serf/Events/EventType.cs:56`). Keep methods only where the call is expensive or has side effects (`Members()` copies; make it a property returning `IReadOnlyList<Member>` and document the snapshot semantics).

### 2.3 Error values instead of exceptions

- `Memberlist.JoinAsync` returns `(int NumJoined, Exception? Error)`; `JoinWithNamedAddressesAsync` the same; `LeaveAsync` returns `Task<Exception?>`; `LeaveManager` returns `LeaveResult { Success, Error }` (`Memberlist/Memberlist.Network.cs:936-1036`, `LeaveResult.cs`). Return `JoinResult` (count plus the per-address failures) and throw a dedicated `JoinFailedException : AggregateException` when nothing was reachable; throw from leave.
- `IMergeDelegate.NotifyMerge` (memberlist) returns `string?`; the Serf-level `IMergeDelegate.NotifyMerge` returns `Task<string?>`; `IAliveDelegate.NotifyAlive` returns `string?` (and is ignored, defect 1). Replace with `ValueTask` methods that throw `MergeRejectedException`/`NodeRejectedException`, or return `bool` with a reason via `out`.
- `SerfValidationHelper.ValidateNodeName` returns an error message; `MergeDelegate.NodeToMember` returns `(Member?, string?)`; `SerfMessageEncoder.EncodeMessage` swallows exceptions and returns an empty array that every caller checks with `raw.Length > 0`. Throw `ArgumentException`/`SerfProtocolException`.
- Generic exceptions: `new Exception(...)` is thrown or returned as the error value in `Memberlist.Network.cs:1017,1237` and `Memberlist.cs:122`; `CA1032` lists 8 exception types lacking standard constructors. Introduce `SerfException`, `MemberlistProtocolException`, `JoinFailedException`, `QueryException`.

### 2.4 Go channels in the public surface

`Config.EventCh : ChannelWriter<IEvent>`, `Serf.IpcEventReader`, `QueryResponse.AckCh`/`ResponseCh`, `NetTransport.PacketChannel`/`StreamChannel`, `RpcStreamHandle.Records`, `ChannelEventDelegate`, `BroadcastNotifyChannel`, `SerfQueries.Create` returning a `ChannelWriter`, `Snapshotter.NewSnapshotterAsync` returning `(ChannelWriter InCh, Snapshotter Snap)`.

Replace with .NET idioms: `Serf.Events` as an `IAsyncEnumerable<IEvent>` subscription (multi-subscriber, each with its own bounded buffer and an explicit overflow policy) or `event EventHandler<SerfEventArgs>`; `QueryResponse.Acks`/`Responses` as `IAsyncEnumerable<...>` plus a `Task Completion`; keep `System.Threading.Channels` internal. This also fixes defect 5.

### 2.5 Mutable model types leak internal state

- `Serf.Members()` and `LocalMember()` return the live `Member` objects held in `MemberManager` (`Serf/Serf.cs:167-175`, `491`); `Member` has public setters and a settable `Dictionary Tags`, so a caller can mutate cluster state. Make `Member` a `sealed record` with `IReadOnlyDictionary<string,string> Tags`, return copies/immutable snapshots.
- Same for `Node`/`NodeState` (`Memberlist.Members()` returns internal `Node` instances; `IEventDelegate` doc says "must not be modified" instead of enforcing it), `Coordinate` (`double[] Vec` settable), `KeyResponse` dictionaries, `MemberEvent.Members : List<Member>`, `QueryParam.FilterTags`, `MessagePushPull` lists, `AgentConfig` arrays/lists, `Config.Tags`, `MetricLabel[] MetricLabels`.
- `CA2227` (27 settable collection properties), `CA1819` (50 array properties), `CA1002` (33 `List<T>` parameters/returns). Use `IReadOnlyList<T>`, `IReadOnlyDictionary<K,V>`, `ImmutableArray<T>`/`ReadOnlyMemory<byte>` for payloads.

### 2.6 Async naming and semantics

- `Async` suffix on synchronous methods: `Serf.UserEventAsync` (returns `Task.CompletedTask`), `Serf.QueryAsync` (`Task.FromResult`), `RpcServer.StartAsync`, `Snapshotter.LeaveAsync`, `ServiceRegistry.RegisterInstanceAsync/DeregisterInstanceAsync/UpdateHealthStatusAsync`, `NSerfServiceProvider.StartAsync/StopAsync/DiscoverServicesAsync`. Either make them genuinely asynchronous or drop the suffix and the `Task`.
- Missing suffix on asynchronous methods: `KeyManager.InstallKey/UseKey/RemoveKey/ListKeys`, `QueryResponse.SendAck/SendResponse`, `Memberlist.SendToAddress`.
- `CancellationToken` is absent from most public asynchronous methods (`Serf.JoinAsync`, `LeaveAsync`, `QueryAsync`, `UserEventAsync`, `SetTagsAsync`, `RemoveFailedNodeAsync`, `KeyManager.*`, `RpcClient.QueryAsync` has it last after six parameters) and, where present, is not last (`CA1068`, 4 sites).
- `ValueTask` for hot, usually-synchronous paths (`SendPacketAsync`, `WriteToAddressAsync`).

### 2.7 Events

`SerfAgent.EventReceived : event Action<IEvent>` and `RetryJoinExhausted : event Action<int>` (`Agent/SerfAgent.cs:46,54`) violate `CA1003`; use `EventHandler<SerfEventArgs>` / `EventHandler<RetryJoinExhaustedEventArgs>` or expose a `Task RetryJoinExhausted` completion.

### 2.8 Nested and misplaced public types

`ScriptInvoker.ScriptResult`, `CircularLogWriter.ILogHandler`, `InMemoryMetrics.Sample`/`Duration`, `OutputFormatter.OutputFormat` (`CA1034`). Move them to top level. `Serf/QueryCollection.cs`, `UserEventCollection.cs`, `MemberState.cs`, `NodeIntent.cs`, `Broadcast.cs` mix wire DTOs and internals in the same namespace as the public API; group wire types under `NSerf.Serf.Wire` (internal) and public models under `NSerf.Serf.Model`.

### 2.9 Configuration model

- Four overlapping configuration types with independent defaults: `NSerfOptions` → `AgentConfig` → `Serf.Config` → `MemberlistConfig` (`Extensions/NSerfOptions.cs:141-172` maps one to the next by hand). Unify on one options graph bound through `IOptions<T>` with `IValidateOptions<T>`; `AgentConfig.Merge` with five hand-written `Merge*` methods and `TimeSpan.Zero`/`0`/`""` sentinels (`Agent/AgentConfig.cs:79-181`) should become nullable properties so "not set" is explicit.
- `Serf.CreateAsync` mutates the caller's config (`config.MemberlistConfig.Name`, `Delegate`, `DelegateProtocolVersion` and the other delegates; `Serf/Serf.cs:320-327`) and `Memberlist.Create` writes the auto-assigned port back into `config.BindPort` (`Memberlist.cs:160-161`). Copy, validate, then freeze.
- `Config.Init()` is a no-op (`??=` on non-nullable properties); `DefaultConfig()`/`DefaultLANConfig()` duplicate initializers; `MsgpackUseNewTimeFormat`, `DNSConfigPath`, `HandoffQueueDepth`, `SecretKey`, `MaxQueueDepth`, `MinQueueDepth` and `RecentIntentTimeout` are never read. Remove or implement (see defects 3, 4).
- `ProtocolVersionMap` static class plus `Serf.ProtocolVersionMin/Max` constants duplicated in `Config.cs` and `Serf.cs`.
- `AgentConfig.Protocol` is an `int` cast to `byte`; `AgentConfig.Role` is documented deprecated but merged; `EventScriptConfig` is unused.

### 2.10 Addresses

Every address is a `"host:port"` string parsed with `Split(':')` (`NetTransport.WriteToAddressAsync`, `DialAddressTimeoutAsync`, `RpcServer.StartAsync`, `SerfAgent.SetBind/SetAdvertise`, `AgentConfig.AddrParts`, `Query.cs`). This breaks on IPv6 literals and parses on every packet send. Use `IPEndPoint` (`IPEndPoint.TryParse`) in the transport and expose `EndPoint` on `Member`/`Node`; keep string parsing at the configuration boundary only. The `Address { Addr, Name }` class becomes a `readonly record struct NodeAddress(IPEndPoint EndPoint, string? Name)`.

### 2.11 Time

`DateTime.UtcNow` (14) and `DateTimeOffset.UtcNow` (26) are mixed (`QueryResponse.Deadline` and `Events.Query.Deadline` are `DateTime`, `MemberInfo.LeaveTime` is `DateTimeOffset`, `MemberState.LeaveTime` is `DateTime`); `Events.Query` uses `Deadline == default` as an "already responded" sentinel (`Serf/Events/Query.cs:117`). Standardise on `DateTimeOffset`, inject `TimeProvider` (built into .NET 8+) so timers, deadlines and the tests' sleeps become deterministic, use `Stopwatch`/`Environment.TickCount64` for elapsed time.

### 2.12 CLS and numeric types

`uint`/`ulong` in public signatures (`Memberlist.Incarnation`, `SequenceNum`, `NextSequenceNum()`, `Query.Id`, `MessageQuery.ID`, `RpcClient.QueryAsync` returning `ulong`, `QueryRequest.Timeout : uint`); `CoordinateConfig.Dimensionality : uint`. Decide whether CLS compliance matters; if it does, use `int`/`long` at the boundary and keep unsigned types on the wire types only. `ID`→`Id` casing (`MessageQuery.ID`, `RespondRequest.ID`, `StreamEvent.QueryID`).

---

## 3. Concurrency and asynchronous code

1. **Sync-over-async**: `Memberlist.Dispose` (`Task.Run(ShutdownAsync).GetAwaiter().GetResult()`, `Memberlist.cs:632`), `NetTransport.Dispose` (`ShutdownAsync().GetAwaiter().GetResult()`, line 465), `AgentMdns.Dispose` (`_runTask.Wait`), `NSerfServiceProvider.StopAsync` (`_eventLoopTask.Wait`), `ScriptInvoker.WaitForCompletionAsync` (`Task.Run(() => process.WaitForExit(ms))`, use `WaitForExitAsync` with `CancelAfter`), `MergeDelegate` (defect 11), `RpcEventHandler`/`RpcLogHandler` (`_writeLock.Wait`, synchronous `Stream.Write`). Rule: types that own asynchronous resources implement only `IAsyncDisposable`; a synchronous `Dispose` must never block (`CA1849` 7 sites).
2. **Fire-and-forget tasks**: 29 `Task.Run` and a dozen `_ = ...` discards (`Serf.CreateAsync` rejoin loop lines 418-453, `Query.cs:111,220,279,290`, `InternalQueryHandler.cs:118,154`, `CoalesceLoop.cs:37`, `ScriptEventHandler.cs:43`, `AgentMdns:121,172`, `NSerfServiceProvider:68`, `PacketHandler:272,348,360,375`, `Events/Query.cs:186`, `Delegate.cs:182`). Track them in the owning object, observe faults through one `TaskExtensions.Forget(ILogger)` helper, and await them on shutdown so `ShutdownAsync` really means "everything stopped".
3. **`Task.Factory.StartNew(async () => ..., LongRunning).Unwrap()`** for asynchronous loops (`Memberlist.Background.cs` ×5, `BackgroundTasks.cs` ×3, `Snapshotter.cs` ×2, `NSerfServiceProvider.cs:81`, `RpcServer.cs:67` per session). `LongRunning` creates a dedicated thread that is released at the first `await`; it only costs a thread creation. Use `Task.Run` (or a small `BackgroundLoop` helper that owns the loop, the token, the fault logging and the stop).
4. **Schedulers**: `Task.Delay(interval, token)` loops in `SetupGossipTask`, `SetupPushPullTask`, `HandleReapAsync`, `HandleReconnectAsync`, `HandleQueueMonitorAsync`, `RetryJoinAsync` drift by the work time; use `PeriodicTimer` (already done for the probe loop). `Serf.RegisterQueryResponse` schedules cleanup with `Task.Delay(timeout).ContinueWith(...)` (`Query.cs:111`) instead of a timer with cancellation.
5. **Locking design**: `Memberlist` exposes `NodeLock`, `Nodes`, `NodeMap`, `NodeTimers`, `AckHandlers`, `NumNodes` as `internal` fields that `StateHandlers`, `PacketHandler`, `LeaveManager` and `Memberlist.Network` mutate directly under an externally shared lock (Go struct with package visibility). Encapsulate node state in one `NodeTable` type with `TryGet/Upsert/MarkDead/Snapshot` methods that own the lock. `NodeMap` is a `ConcurrentDictionary` that is also guarded by `NodeLock` (double synchronisation); `NodeTimers` is `ConcurrentDictionary<string, object>` with `is Suspicion` casts; `Node.State` duplicates `NodeState.State` ("CRITICAL: Update Node.State too" comments in `StateHandlers.cs:523,530`).
6. **`LockHelper`** (`Serf/Helpers/LockHelper.cs`) reimplements Go's `defer Unlock()`; C# has `lock`, and `SemaphoreSlim`+`try/finally` or a `using`-scoped `AsyncLock`. `MemberManager.ExecuteUnderLock(accessor => ...)` allocates an accessor per call and takes the **write** lock for reads (`Managers/MemberManager.cs:18-40`), so the `ReaderWriterLockSlim` never provides concurrency; `EventManager` and `ClusterCoordinator` (a `SemaphoreSlim` guarding one enum field, plus `ObjectDisposedException` swallowed in every method) have the same shape. Replace with a plain `lock` or `Interlocked`/`Volatile` for the enum.
7. **`ConfigureAwait(false)`** is absent everywhere (`CA2007`, 453 sites). Adopt it library-wide (or disable the rule deliberately and document that the library never resumes on a captured context).
8. **`Suspicion.Confirm`** fires the timeout callback with `Task.Run` while holding `_lock` (`Memberlist/Suspicion.cs:92`); `_n` is read with `Volatile`/`Interlocked` although every access is already under the lock.
9. **Disposal**: `Serf.DisposeAsync` just calls `Dispose`, which cancels the token and disposes locks but never calls `ShutdownAsync` (memberlist and transport stay alive when a caller only disposes); `SerfAgent` reuses `_disposed` as its "shut down" flag; `RpcClient` swaps between `_readerCts`/`_readerTask` fields without volatility; `KeyManager._lock`, `Serf._joinLock` and others are never disposed; `NetTransport.DialAddressTimeoutAsync` wraps `client.Client` in a `NetworkStream(ownsSocket: true)` and drops the `TcpClient` wrapper (`NetTransport.cs:313`); `AckHandler` implements the `Dispose(bool)` pattern on a sealed class with no finalizer (`CA1063`, 15 sites; `CA2213` 7 undisposed fields; `CA1001` 1).
10. **Cancellation**: `GossipAsync` substitutes the shutdown token when the caller passes `CancellationToken.None` (`Memberlist.Network.cs:455`), an implicit contract; `SendToAddress` and the relay path pass `CancellationToken.None`; the RPC session and script invoker have no per-operation timeouts beyond the socket ones.

---

## 4. Error handling and logging

1. **Catch-all handling** (`CA1031`, 159 sites): most `catch (Exception ex)` blocks log and continue, which hides programming errors (a `NullReferenceException` inside `HandleAliveNode` is logged as "Failed to process Alive message" and the packet loop carries on with corrupted state). Catch the exceptions you expect (`SocketException`, `IOException`, `MessagePackSerializationException`, `OperationCanceledException`), let the rest crash the loop and surface through the background-task fault path. Empty or comment-only `catch` blocks in `NetTransport.cs:179,184`, `ScriptInvoker.cs:163,195`, `CoalesceLoop.cs:115`, `RpcEventHandler.cs:98` and `RpcLogHandler.cs:79` must at least log at Debug.
2. **Log-and-rethrow** duplicates every stream error two or three times (`HandleStreamAsync` → `ProcessInnerMessageAsync` → `HandleUserMsgStreamAsync` each log then `throw`).
3. **Nullable loggers**: `ILogger?` fields in 13 types, with `?.` at all 453 logging call sites. Inject a non-null `ILogger` (default `NullLogger.Instance`, or `ILogger<T>` from DI) and drop the `?.`.
4. **Logging performance** (`CA1848` 459, `CA1873` 175): per-packet `LogDebug` calls format arguments (`from`, `buf[0]`, `string.Join(...)`) even when Debug is off. Use the `LoggerMessage` source generator for the hot paths (packet handler, gossip, probe, state handlers) and `IsEnabled` guards elsewhere. `PacketHandler` repeats `if (!memberlist.Config.StealthUdp) logger?.LogDebug(...)` twenty-eight times; wrap once.
5. **Message style**: bracketed component prefixes (`"[Serf/Snapshot] ✓ Snapshotter created"`, `"[GOSSIP] ..."`, `"[Serf.Delegate] *** GetBroadcasts called ... ***"`, `"[HandleDeadNode] Graceful leave OVERRIDING failure: {Node} Dead→Left"`) belong in the logger category and scopes, not in the text; remove decorations and Unicode arrows; use consistent event ids.
6. **Levels**: every member state transition logs at Information (`MemberStateMachine`, `IntentHandler`, `ClusterCoordinator`, `NodeEventHandler`, `Snapshotter` replay lines); `Serf.CreateAsync` and its rejoin path contain 19 Information-level calls. Demote to Debug; reserve Information for join/leave/shutdown.
7. **`Debug.WriteLine`** in `TagEncoder.DecodeTags` hides decode failures in Release; `RpcServer.AcceptClientsAsync` has a `catch (Exception) { // Log error if logger available }` with no logger.

---

## 5. Idiomatic C# and code quality

### 5.1 Duplicated implementations (pick one, delete the other)

- Compound messages: `Messages/CompoundMessage.cs` (List/BinaryWriter) and `Messages/MessageEncoder.MakeCompoundMessage(s)/DecodeCompoundMessage` (array/span).
- Node selection: `Common/CollectionUtils.KRandomNodes/ShuffleNodes/MoveDeadNodes` (arrays, unused) and `State/NodeStateManager` (lists); `QueryHelpers.KRandomMembers` is a third copy for `Member`. Also two `RandomOffset` (`MemberlistMath`, `NodeStateManager`), the former with modulo bias on `Random.Shared.Next()`.
- Serf message encoding: `MessageCodec` (mostly unused) and `SerfMessageEncoder`; `TagEncoder` wrapped by `SerfMessageEncoder.EncodeTags/DecodeTags`; `Serf.EncodeMessage(MessageType, object)` untyped.
- Event-type names: `EventTypeExtensions.String`, `EventFilter.GetEventTypeName`, `ScriptInvoker.GetEventTypeName`, `MemberEvent.ToString` (four switch tables).
- Log-level prefix parsing: `LogWriter.ShouldWrite`, `FilteredLogHandler.ShouldWrite`, `LogLevelExtensions.FromString/TryFromString`.
- Protocol version vectors: `Vsn[0..5]` ⇄ `PMin..DCur` copied in `Memberlist.cs`, `Memberlist.Network.cs` (×2), `StateHandlers.cs` (×4), `RefuteNode`; introduce `readonly record struct ProtocolVersions(byte PMin, byte PMax, byte PCur, byte DMin, byte DMax, byte DCur)` with `ToVsn()/FromVsn()`.
- Wire DTO vs domain DTO pairs with hand mapping: `Alive`/`AliveMessage`, `Suspect`/`SuspectMessage`, `Dead`/`DeadMessage` mapped in `EncodeBroadcastNotify`, `QueueMessage`, `MergeRemoteState`. Use one type.
- Four copy-pasted key handlers in `SerfQueries` (`InternalQueryHandler.cs:265-471`) and four `*WithOptions` wrappers in `KeyManager` whose `opts` is always `null`; `Serf.GetCoordinate(string)` and `GetCachedCoordinate(string)` are identical.
- `SerfValidationHelper.ValidateUserEventSize` duplicates the inline checks in `Serf.UserEventAsync`; `SerfMetricsRecorder` has eight methods but `Serf` calls `Config.Metrics.IncrCounter` directly with hand-written label arrays.
- Address formatting `$"{addr}:{port}"` throughout and `Split(':')` parsing in 7 places.

### 5.2 Dead code and unused packages

`ConfigLoader.LoadFromArgs` (a second CLI parser), `CollectionUtils`, `NodeStateManager.MoveDeadNodes/ShuffleNodes`, `MessageCodec` (only `EncodeRelayMessage` is called), `SerfMessageEncoder.TryDecodeMessage`, `TagEncoder.IsTagEncoded`, `MessageEncoder.MakeCompoundMessages`, `CompressMessage` + `CompressionType.Lzw`, `ChannelEventDelegate`, `NodeIntent`, `MemberState`, `KeyRequestOptions`, `StateSnapshot`/`GetStateSnapshot`/`ExecuteInLeavingState`/`ExecuteIfNotShutdown`, `EventScriptConfig`, `FilterNode`, `CoalesceLoop` (defect 3), `CoalesceLoop.FlushReason`, `EncryptionVersion`, `MessageConstants.BlockingWarning/MaxPushStateBytes/MaxPushPullRequests/UdpPacketBufSize` (`NetTransport` keeps its own private copy of the buffer size; the push/pull concurrency limit Go enforces is not implemented), `Config.Init`, `ServiceDiscoveryOptions`, `IServiceExporter`, the `Moq` package (referenced, never used), `KeyManager.KeyRequestOptions.RelayFactor`. Delete or implement; a `PublicAPI.Shipped.txt` (Microsoft.CodeAnalysis.PublicApiAnalyzers) will keep the surface from silently growing again.

### 5.3 Allocation and I/O

- `buf[1..]` range slices copy every packet (`PacketHandler.HandleCommand`, `HandleCompound`, `ShouldProcessQuery`); `MessageEncoder.Decode` calls `buffer.ToArray()` for every message although MessagePack can read a `ReadOnlyMemory<byte>`; `Delegate.DecodeMessage` and `PingDelegate` do the same; `SecurityTools.EncryptPayload` builds output through `List<byte>.AddRange` and `DecryptPayload` copies nonce, ciphertext and tag with `ToArray()`. Use `ReadOnlyMemory<byte>`/spans and `AesGcm` on spans; pool with `ArrayPool<byte>`.
- TCP framing is hand-rolled three times with `new byte[4]`, `BitConverter` and `Array.Reverse` (`Memberlist.Network.cs:522-537, 840-846, 1140-1159`), with manual read-until-full loops at lines 545, 674, 1167 and 1375; use `BinaryPrimitives.WriteInt32BigEndian`, `Stream.ReadExactlyAsync`, and one `FramedStream` helper. `PacketHandler.ReverseBytes` reimplements `BinaryPrimitives.ReverseEndianness`. `MemoryStream` + `ToArray` copies in every encoder.
- `new StateHandlers(this, _logger)` is allocated per packet and per probe (`PacketHandler.QueueMessage:468`, `Memberlist.Network.cs:306,1081`); `new Regex` per query per tag; LINQ inside `NodeLock` (`Nodes.Any`, `Where(...).Select(...)` in `SendLocalStateAsync`); `Members()` rebuilt with LINQ on every call by the reconnect/reap loops; `Keyring.KeysEqual` uses `Where((t, i) => ...)`; use `SequenceEqual`/`CryptographicOperations.FixedTimeEquals`.
- `UdpClient`/`TcpListener` are the legacy wrappers; `Socket.ReceiveFromAsync` with a pooled buffer avoids a per-packet array.

### 5.4 Naming conventions (needs an `.editorconfig` enforced in the build)

- Go field names kept verbatim: `Vsn`, `PMin/PMax/PCur/DMin/DMax/DCur`, `CC`, `ID`, `LTime`, `Addr`, `Msg`, `Buf`, `NumNodes`/`EstNumNodes`, `kNodes`, `UnmarshalTags`, `EncryptBytes()`, `Finished()`, `String()`.
- Acronym casing: `TCPTimeout`, `UDPBufferSize`, `DNSConfigPath`, `RPCAddr`/`RPCAuthKey` (`NSerfOptions`), `CIDRsAllowed`, `ToIpEndPoint` next to `IPAddress`.
- Suffix rules (`CA1711`, 15): `*Collection`, `*Queue`, `*Delegate` on classes that are not delegates, `*EventArgs` misuse.
- `Manager`/`Coordinator`/`Helper`/`Utils`/`Handlers` classes (`MemberManager`, `EventManager`, `ClusterCoordinator`, `LeaveManager`, `NodeStateManager`, `LockHelper`, `SerfQueryHelper`, `SerfValidationHelper`, `SerfMetricsRecorder`, `QueryHelpers`, `CollectionUtils`, `NetworkUtils`, `LoggingUtils`, `StateHandlers`, `PacketHandler`): procedural bags of functions operating on another object's internals. Fold them into the owning type or make them real services with a single responsibility.
- Test-hook and diary vocabulary in identifiers and comments: `MessageDropper`, "Phase N:", "Fix:", "CRITICAL:", "for now" (25 such comments), plus 219 comments that narrate the Go implementation ("Maps to: Go's hasAliveMembers()", "Corresponds to state.go (lines …)"). Keep one `// Ported from …` header per file and delete the rest.
- `LangVersion` is pinned to 12 while the SDK supports 14; primary constructors, collection expressions and `field` are already used in places, so pick `latest` and apply it consistently (`ScriptResult`, `EventScript`, `AgentConfig` still use the old constructor style).

### 5.5 Structure

- `Serf` is a `partial class` split over `Serf.cs`, `Query.cs`, `BackgroundTasks.cs`, `Serf.Stats.cs`; `Memberlist` over three files. Partials are being used as Go file boundaries. Extract real collaborators: `QueryCoordinator` (query registry, acks, filters), `Reaper`/`Reconnector`, `Gossiper`/`Prober`/`StreamServer`, `PushPullSynchronizer`.
- `SerfAgent` (749 lines) builds configuration, loads files, owns the keyring, dispatches events, retries joins and hosts the RPC server; `RpcSession` (1,163 lines) contains every command with the same `WaitAsync/Serialize/Write/Release` block repeated 14 times; `Memberlist.Network.cs` (1,383 lines) mixes probing, gossip, streams, push/pull and framing. Split by responsibility and dispatch RPC commands through a handler map.
- Static process-wide state: `NetTransport._nextPort` and Windows port-range probing live in production code for the benefit of tests (`Transport/NetTransport.cs:39-41,197-234`); `AgentMdns.SharedMdns`; `ServiceDiscoveryHttpMessageHandler.RoundRobinCounters`. Inject a port allocator/multicast service instead.
- Public types that should be internal (`CA1515` in the CLI: 19; in the library `StateHandlers`, `PacketHandler`, `LeaveManager`, `TransmitLimitedQueue`, `AckNackHandler`, `Suspicion`, `Awareness`, `SerfQueries`, `MemberStateMachine`, `TransitionResult`, `IntentHandler` interfaces, `LockHelper`, the `*MessageBroadcast` classes, `BroadcastNotifyChannel`, `LimitedBroadcast`, the `Serf.Helpers` namespace). Everything the README does not document should be `internal`; `InternalsVisibleTo("NSerfTests")` already exists.

### 5.6 Nullability

`Serf.Logger` is `ILogger?` although `Config.Logger` guarantees non-null; `Serf.Memberlist?` is dereferenced with `!` in 4 places; `EventScript.Filter`, `MemberInfo.StateMachine` and `TransmitLimitedQueue.Broadcast` are initialised to `null!`; `ServiceRegistry.GetService(string? serviceName)` declares nullable then `ThrowIfNull`; `Serf.CreateAsync(Config? config)` accepts null only to throw. Prefer non-nullable parameters, `required` members, and no `!` outside tests.

### 5.7 Randomness and culture

`new Random()` per call in `QueryHelpers.KRandomMembers` and `Coordinate.UnitVectorAt`; shared `Random` field in `Serf` (defect 10); `Random.Shared` elsewhere (`CA5394` 16). `ToUpper()`/`ToLower()`/`StartsWith(string)`/`int.Parse` without `StringComparison`/`CultureInfo` (`CA1308/1311/1310/1307/1305`, 42 sites): `ScriptInvoker.SanitizeTagName`, `LogLevelExtensions.FromString`, `NetTransport`, `AgentConfig.AddrParts`, `SerfQueries.StreamAsync`.

### 5.8 Magic numbers

Ports 7946/7373 appear on 20 lines of the library and CLI; the 512-byte meta limit is hard-coded again in `MergeDelegate.ValidateMemberInfo`; 10-second shutdown/leave timeouts in `SerfAgent.ShutdownAsync`, `Snapshotter` (5 s), `AgentMdns` (2 s), `RpcClient.TearDownConnectionAsync` (2 s), `AgentCommand` (3 s); channel sizes 64/1024/2048; `3 * n` probe attempts; the `1024`-byte TCP read. Name them or move them to options.

### 5.9 Project hygiene

- Add `.editorconfig` (naming, `IDE00xx` style, file headers) and `Directory.Build.props` with `LangVersion`, `Nullable`, `ImplicitUsings`, `TreatWarningsAsErrors`, `AnalysisLevel=latest`, `AnalysisMode=Recommended`, `EnforceCodeStyleInBuild`, `Deterministic`, `ContinuousIntegrationBuild`, SourceLink; the test projects currently build with warnings allowed.
- `NoWarn CS1591` suppresses missing XML docs for the whole public surface; either document it or shrink it (5.5).
- The library itself has no `Console` writes (the agent runner already routes status through a `TextWriter`), but the CLI writes to `Console` directly in every command; inject `TextWriter`/`IConsole` so tests stop redirecting the process-wide console (the reason `ThreadSafeStringWriter` had to be added to both test projects).

### 5.10 Tests

- 1,474 test methods in 191 classes across 50k lines; the largest file (`Memberlist/StateTests.cs`) is 2,897 lines. Unit and integration tests share one project with parallelisation disabled, so the smallest change costs a 13-minute run. Split into `NSerf.UnitTests` (parallel, no sockets, `MockTransport`, `TimeProvider`) and `NSerf.IntegrationTests` (`[Collection]` per shared port range, `[Trait("Category","Integration")]` as the CLI tests already do).
- Assertion style is mixed (2,469 FluentAssertions calls, 1,306 `Assert.*`); 91 `Console.WriteLine` diagnostics; 5 `TODO`s; test names range from Go names to `Test_1_1_1_BasicTcpConnection` to `Serf_JoinAndLeave_ShouldWork`; a test-only `SerfConfig` type lives in `TestHelpers`; 313 fixed `Task.Delay` sleeps that a `TimeProvider`-driven design would remove.
- Most cluster tests construct two or three real nodes inline with copy-pasted `Config`/`MemberlistConfig` blocks; a `ClusterFixture` (xunit `IAsyncLifetime`) that owns ports, nodes and disposal would cut hundreds of lines and the leaks that currently rely on `GC`.

### 5.11 CLI

- Thirteen of the sixteen commands re-declare `--rpc-addr` and `--rpc-auth` and the connect/try/catch/exit-code boilerplate; some rethrow after printing (`query`, `monitor`, `rtt`, `force-leave`, `reachability`), others return 1. Centralise in a `RpcCommandBase` (shared options, `ConnectAsync`, uniform exit codes).
- `rtt` prints "Coordinate information for X: Available" without computing a distance (Go computes the estimated RTT between two nodes' coordinates); `reachability` only prints membership status (Go runs a `_serf_ping` query and reports which nodes acknowledged). Implement or rename.
- `keygen` and `config-secrets` duplicate key generation; `ConfigLoader.LoadFromArgs` duplicates System.CommandLine.

### 5.12 Documentation

`docs/cli.md` does not mention `--discover`, `--log-level`, `--retry-join`, `--snapshot` or `--rejoin`; `docs/serf-config.md` should list only the options that have an effect once defects 3 and 4 are settled; XML docs on the public surface once 5.5 has shrunk it.

---

## 6. Suggested order of work

1. **Hygiene (1-2 days, no behaviour change):** `Directory.Build.props`, `.editorconfig`, analyzers at `Recommended`, delete the dead code in 5.2, remove diary comments, `ConfigureAwait` policy, drop `Moq`.
2. **Defects (section 1):** each with a failing test first; 1, 2, 5, 6, 7, 8, 11 and 13 change observable behaviour and should ship as a patch release with release notes.
3. **Internal refactors (sections 3-5, non-breaking):** encapsulate memberlist node state, replace `LockHelper`/`MemberManager` accessor pattern, background-loop helper with tracked tasks, framing helper, `ProtocolVersions`, single encoder, logging source generators, `TimeProvider`.
4. **Public API v1.0 (section 2):** renames, records, exceptions, `IAsyncEnumerable` events, unified options, `PublicAPI.Shipped.txt`; keep `[Obsolete]` shims for one minor release where cheap.
5. **Test split (5.10):** unit/integration projects, cluster fixture, deterministic time.
