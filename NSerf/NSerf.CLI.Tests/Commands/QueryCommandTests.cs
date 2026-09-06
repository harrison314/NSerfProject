// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using NSerf.CLI.Commands;
using NSerf.CLI.Tests.Fixtures;
using NSerf.CLI.Tests.Helpers;

namespace NSerf.CLI.Tests.Commands;

[Trait("Category", "Integration")]
[Collection("Sequential")]
public class QueryCommandTests
{
    [Fact(Timeout = 10000)]
    public async Task QueryCommand_Dispatches()
    {
        await using var fixture = new AgentFixture();
        await fixture.InitializeAsync();

        var rootCommand = new RootCommand();
        rootCommand.Add(QueryCommand.Create());

        // The command runs until the query is done (Go semantics), so keep the query timeout short
        var args = new[] { "query", "--rpc-addr", fixture.RpcAddr!, "--timeout", "1", "test-query" };

        var (exitCode, output, error) = await CommandTestHelper.ExecuteCommandAsync(rootCommand, args);

        if (exitCode != 0)
        {
            Console.WriteLine($"EXIT CODE: {exitCode}");
            Console.WriteLine($"OUTPUT: {output}");
            Console.WriteLine($"ERROR: {error}");
        }
        Assert.Equal(0, exitCode);
        Assert.Contains("Query 'test-query' dispatched", output);
    }

    /// <summary>
    /// Port of Go's 'serf query' text output: acks and responses are printed as they arrive
    /// and totals are printed once the query is done.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task QueryCommand_PrintsAcksResponsesAndTotals()
    {
        await using var fixture = new AgentFixture();
        await fixture.InitializeAsync();

        var responder = new QueryResponder(System.Text.Encoding.UTF8.GetBytes("pong"));
        fixture.Agent!.RegisterEventHandler(responder);

        var rootCommand = new RootCommand();
        rootCommand.Add(QueryCommand.Create());

        var args = new[] { "query", "--rpc-addr", fixture.RpcAddr!, "--timeout", "2", "test-query", "ping" };

        var (exitCode, output, error) = await CommandTestHelper.ExecuteCommandAsync(rootCommand, args);

        Assert.True(exitCode == 0, $"exit code {exitCode}, output: {output}, error: {error}");
        var nodeName = fixture.Agent.NodeName;
        Assert.Contains($"Ack from '{nodeName}'", output);
        Assert.Contains($"Response from '{nodeName}': pong", output);
        Assert.Contains("Total Acks: 1", output);
        Assert.Contains("Total Responses: 1", output);
    }

    private sealed class QueryResponder(byte[] payload) : NSerf.Agent.IEventHandler
    {
        public void HandleEvent(NSerf.Serf.Events.IEvent @event)
        {
            if (@event is NSerf.Serf.Events.Query query)
            {
                _ = query.RespondAsync(payload);
            }
        }
    }
}
