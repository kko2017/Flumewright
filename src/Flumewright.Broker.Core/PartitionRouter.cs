using System;

namespace Flumewright.Broker.Core;

public static class PartitionRouter
{
    // partitionCount > 0. Returns [0, partitionCount).
    public static int ForKey(ReadOnlySpan<byte> partitionKey, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");
        }

        uint hash = 2166136261;
        for (int i = 0; i < partitionKey.Length; i++)
        {
            hash ^= partitionKey[i];
            hash *= 16777619;
        }

        return (int)(hash % (uint)partitionCount);
    }

    // when no key: round-robin using a caller-supplied counter
    public static int RoundRobin(long monotonicCounter, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than zero.");
        }

        long positiveCounter = monotonicCounter & long.MaxValue;
        return (int)(positiveCounter % partitionCount);
    }
}
