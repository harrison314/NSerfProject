// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using MessagePack;

namespace NSerf.Client.Responses;

/// <summary>
/// Response to the install-key / use-key / remove-key / list-keys RPC commands.
/// Maps to: Go's keyResponse in const.go (PrimaryKeys is an NSerf extension).
/// </summary>
[MessagePackObject]
public class KeyResponse
{
    /// <summary>
    /// Number of nodes that were expected to respond.
    /// </summary>
    [Key(0)]
    public int NumNodes { get; set; }

    /// <summary>
    /// Number of nodes that reported an error.
    /// </summary>
    [Key(1)]
    public int NumErr { get; set; }

    /// <summary>
    /// Number of responses received.
    /// </summary>
    [Key(2)]
    public int NumResp { get; set; }

    /// <summary>
    /// Base64 key -> number of nodes that have it installed.
    /// </summary>
    [Key(3)]
    public Dictionary<string, int> Keys { get; set; } = new();

    /// <summary>
    /// Node name -> message (usually an error) reported by that node.
    /// </summary>
    [Key(4)]
    public Dictionary<string, string> Messages { get; set; } = new();

    /// <summary>
    /// Base64 key -> number of nodes that use it as their primary key.
    /// </summary>
    [Key(5)]
    public Dictionary<string, int> PrimaryKeys { get; set; } = new();
}
