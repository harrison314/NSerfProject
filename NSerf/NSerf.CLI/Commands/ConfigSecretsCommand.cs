// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.CommandLine;
using System.Security.Cryptography;
using System.Text.Json;
using NSerf.CLI.Helpers;

namespace NSerf.CLI.Commands;

/// <summary>
/// Config-secrets command - generates secrets suitable for AgentConfig JSON.
/// Generates:
/// - RPCAuthKey (RPC authentication token)
/// - encrypt_key (gossip encryption key, 32-byte base64)
/// </summary>
public static class ConfigSecretsCommand
{
    public static Command Create()
    {
        var command = new Command("config-secrets", "Generate security secrets for agent configuration");

        var outputFileOption = new Option<string?>("--output-file")
        {
            Description = "Optional path to write the generated JSON instead of printing to stdout"
        };

        command.Add(outputFileOption);

        command.SetAction(async (parseResult, cancellationToken) =>
        {
            var outputPath = parseResult.GetValue(outputFileOption);

            var secrets = GenerateSecrets();
            var json = JsonSerializer.Serialize(secrets, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            if (!string.IsNullOrWhiteSpace(outputPath))
            {
                await File.WriteAllTextAsync(outputPath, json, cancellationToken);
                Console.WriteLine($"Wrote generated secrets to {outputPath}");
            }
            else
            {
                Console.WriteLine(json);
            }

            return 0;
        });

        return command;
    }

    private static Dictionary<string, string> GenerateSecrets()
    {
        var rpcAuthKey = GenerateRandomBase64(32);

        // IMPORTANT: EncryptKey must be exactly 32 bytes to satisfy AgentConfig.EncryptBytes()
        var encryptKey = GenerateRandomBase64(32);

        return new Dictionary<string, string>
        {
            // Matches AgentConfig.RpcAuthKey attribute: [JsonPropertyName("RPCAuthKey")]
            ["RPCAuthKey"] = rpcAuthKey,

            // Matches AgentConfig.EncryptKey with SnakeCaseLower policy: encrypt_key
            ["encrypt_key"] = encryptKey
        };
    }

    private static string GenerateRandomBase64(int byteLength)
    {
        var buffer = new byte[byteLength];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(buffer);
        return Convert.ToBase64String(buffer);
    }
}
