// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using NSerf.CLI.Commands;

var rootCommand = new RootCommand("NSerf - Service orchestration and discovery tool")
{
    // Add commands
    AgentCommand.Create(), // Agent command must be first
    MembersCommand.Create(),
    JoinCommand.Create(),
    LeaveCommand.Create(),
    ForceLeaveCommand.Create(),
    EventCommand.Create(),
    QueryCommand.Create(),
    TagsCommand.Create(),
    InfoCommand.Create(),
    MonitorCommand.Create(),
    KeygenCommand.Create(),
    KeysCommand.Create(),
    RttCommand.Create(),
    ReachabilityCommand.Create(),
    VersionCommand.Create(),
    ConfigSecretsCommand.Create()
};

// The agent command owns signal handling (SIGINT/SIGTERM/SIGHUP with Go's leave semantics), so
// System.CommandLine's Ctrl+C termination timeout must not cut a graceful leave short.
return await rootCommand.Parse(args).InvokeAsync(new InvocationConfiguration
{
    ProcessTerminationTimeout = null
});
