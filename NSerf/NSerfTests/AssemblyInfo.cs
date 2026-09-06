using Xunit;

// Parallel test execution stays DISABLED for this assembly.
//
// Many integration tests bind real loopback sockets on FIXED port numbers (and macOS only routes
// 127.0.0.1, so tests cannot be separated by loopback alias either). Running two of these classes
// at the same time produces "address already in use" bind failures or, worse, cross-talk between
// clusters that happen to share a port. Test files that use fixed ports include:
//
//   - Agent/AgentMdnsTests.cs                                   (7946, advertised via mDNS)
//   - Agent/AgentOperationsTests.cs                             (19946-19947)
//   - Agent/RetryJoinTests.cs                                   (17001-17002)
//   - Serf/SerfJoinLeaveTest.cs                                 (7946 - shared with AgentMdnsTests and
//                                                                 NSerfClusterHttpIntegrationTests)
//   - Integration/AgentIntegrationTests.cs                       (17946, 17374-17377)
//   - Integration/LeaveEventBroadcastTests.cs                    (19001-19013)
//   - Integration/LeaveGossipTests.cs                            (19101-19114)
//   - Integration/LeaveGossipRootCauseTest.cs                    (19201-19202)
//   - Serf/SerfSnapshotTest.cs                                   (32107, 32118)
//   - ServiceDiscovery/NSerfServiceProviderIntegrationTests.cs   (17946, 1795x)
//   - ServiceDiscovery/NSerfServiceProviderAdvancedIntegrationTests.cs
//                                                                (18001-18003, 18101-18105, 18201-18202,
//                                                                 18301-18302, 18501-18502, 18601-18602,
//                                                                 18701-18702, 18801-18803, 19101-19104,
//                                                                 19200-19204 - note the overlap with
//                                                                 LeaveGossipTests)
//   - ServiceDiscovery/Http/NSerfClusterHttpIntegrationTests.cs  (7946-7948, 795x, 796x, 797x, 798x, 7990
//                                                                 plus HTTP listeners on 8001-8050)
//
// The NSerf.CLI.Tests project has the same constraint (e.g. Commands/AgentCommandTests.cs uses
// RPC port 1332). Making these tests parallel-safe requires moving every one of them to port 0
// (OS-assigned) first; until then keep this attribute in place.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
