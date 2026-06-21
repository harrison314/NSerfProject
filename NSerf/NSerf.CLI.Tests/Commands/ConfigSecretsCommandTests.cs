// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using System.Text.Json;
using NSerf.Agent;
using NSerf.CLI.Commands;
using NSerf.CLI.Tests.Helpers;

namespace NSerf.CLI.Tests.Commands;

[Trait("Category", "Unit")]
[Collection("Sequential")]
public class ConfigSecretsCommandTests
{
    [Fact(Timeout = 5000)]
    public async Task ConfigSecretsCommand_PrintsValidJsonToStdout()
    {
        // Arrange
        var rootCommand = new RootCommand();
        rootCommand.Add(ConfigSecretsCommand.Create());

        var args = new[] { "config-secrets" };

        // Act
        var (exitCode, output, error) = await CommandTestHelper.ExecuteCommandAsync(rootCommand, args);

        // Assert
        Assert.Equal(0, exitCode);
        Assert.NotEmpty(output);
        Assert.Empty(error);

        // Parse JSON and validate structure
        var json = output.Trim();
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("RPCAuthKey", out var rpcAuthProp));
        Assert.True(root.TryGetProperty("encrypt_key", out var encryptProp));

        // Base64 + length checks
        var rpcAuth = rpcAuthProp.GetString();
        var encryptKey = encryptProp.GetString();

        Assert.False(string.IsNullOrWhiteSpace(rpcAuth));
        Assert.False(string.IsNullOrWhiteSpace(encryptKey));

        var rpcAuthBytes = Convert.FromBase64String(rpcAuth!);
        var encryptKeyBytes = Convert.FromBase64String(encryptKey!);

        Assert.Equal(32, rpcAuthBytes.Length);
        Assert.Equal(32, encryptKeyBytes.Length);

        // Verify that AgentConfig can deserialize this JSON without error
        var config = JsonSerializer.Deserialize<AgentConfig>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
        });

        Assert.NotNull(config);
        Assert.Equal(encryptKey, config!.EncryptKey);
    }

    [Fact(Timeout = 5000)]
    public async Task ConfigSecretsCommand_WritesJsonToFile_WhenOutputFileSpecified()
    {
        // Arrange
        var rootCommand = new RootCommand();
        rootCommand.Add(ConfigSecretsCommand.Create());

        var tempFile = Path.Combine(Path.GetTempPath(), $"nserf_config_secrets_{Guid.NewGuid():N}.json");
        var args = new[] { "config-secrets", "--output-file", tempFile };

        try
        {
            // Act
            var (exitCode, output, error) = await CommandTestHelper.ExecuteCommandAsync(rootCommand, args);

            // Assert
            Assert.Equal(0, exitCode);
            Assert.Contains("Wrote generated secrets to", output);
            Assert.Empty(error);

            Assert.True(File.Exists(tempFile));
            var json = await File.ReadAllTextAsync(tempFile);
            Assert.False(string.IsNullOrWhiteSpace(json));

            // Parse to ensure it is valid JSON
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.True(root.TryGetProperty("RPCAuthKey", out _));
            Assert.True(root.TryGetProperty("encrypt_key", out _));
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }
}
