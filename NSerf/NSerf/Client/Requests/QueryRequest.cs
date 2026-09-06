// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using MessagePack;

namespace NSerf.Client.Requests;

[MessagePackObject]
public class QueryRequest
{
    /// <summary>
    /// Restrict the query to these node names (empty = all nodes).
    /// </summary>
    [Key(0)]
    public string[] FilterNodes { get; set; } = [];

    /// <summary>
    /// Tag name -> regular expression that a node's tag value must match (empty = no tag filter).
    /// </summary>
    [Key(1)]
    public Dictionary<string, string> FilterTags { get; set; } = new();
    
    [Key(2)]
    public bool RequestAck { get; set; }
    
    [Key(3)]
    public uint Timeout { get; set; }
    
    [Key(4)]
    public string Name { get; set; } = string.Empty;
    
    [Key(5)]
    public byte[] Payload { get; set; } = Array.Empty<byte>();
}
