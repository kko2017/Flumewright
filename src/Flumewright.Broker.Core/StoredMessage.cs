namespace Flumewright.Broker.Core;

public sealed record StoredMessage(
    long Offset,
    IReadOnlyDictionary<string, string> Headers,
    ReadOnlyMemory<byte> Payload);
