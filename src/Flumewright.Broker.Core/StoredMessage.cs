using System;
using System.Collections.Generic;

namespace Flumewright.Broker.Core;

public sealed record StoredMessage(
    int Partition,
    long Offset,
    IReadOnlyDictionary<string, string> Headers,
    ReadOnlyMemory<byte> Payload);
