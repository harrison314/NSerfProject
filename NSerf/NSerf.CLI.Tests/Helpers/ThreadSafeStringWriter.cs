// Copyright (c) BoolHak, Inc.
// SPDX-License-Identifier: MPL-2.0

using System.Text;

namespace NSerf.CLI.Tests.Helpers;

/// <summary>
/// A console capture writer whose content can be read (ToString) while other threads keep writing to it.
/// StringWriter is not thread-safe, so polling it from the test thread would race the agent's log output.
/// </summary>
internal sealed class ThreadSafeStringWriter : TextWriter
{
    private readonly StringBuilder _buffer = new();
    private readonly object _sync = new();

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(char value)
    {
        lock (_sync) _buffer.Append(value);
    }

    public override void Write(char[] buffer, int index, int count)
    {
        lock (_sync) _buffer.Append(buffer, index, count);
    }

    public override void Write(ReadOnlySpan<char> buffer)
    {
        lock (_sync) _buffer.Append(buffer);
    }

    public override void Write(string? value)
    {
        lock (_sync) _buffer.Append(value);
    }

    public override void WriteLine(string? value)
    {
        lock (_sync)
        {
            _buffer.Append(value);
            _buffer.Append(CoreNewLine);
        }
    }

    public override void WriteLine(ReadOnlySpan<char> buffer)
    {
        lock (_sync)
        {
            _buffer.Append(buffer);
            _buffer.Append(CoreNewLine);
        }
    }

    public override string ToString()
    {
        lock (_sync) return _buffer.ToString();
    }
}
