using System;
using FluentAssertions;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class PartitionRouterTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void ForKey_SameKey_ReturnsSamePartition()
    {
        var key = new byte[] { 1, 2, 3, 4 };
        int partition1 = PartitionRouter.ForKey(key, 4);
        int partition2 = PartitionRouter.ForKey(key, 4);

        partition1.Should().Be(partition2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ForKey_PartitionCountOne_ReturnsZero()
    {
        var key1 = new byte[] { 1, 2, 3 };
        var key2 = new byte[] { 4, 5, 6 };

        PartitionRouter.ForKey(key1, 1).Should().Be(0);
        PartitionRouter.ForKey(key2, 1).Should().Be(0);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ForKey_DifferentKeys_DistributeAcrossPartitions()
    {
        int partitionCount = 4;
        var partitionsUsed = new bool[partitionCount];

        for (byte i = 0; i < 100; i++)
        {
            var key = new byte[] { i };
            int partition = PartitionRouter.ForKey(key, partitionCount);
            partition.Should().BeInRange(0, partitionCount - 1);
            partitionsUsed[partition] = true;
        }

        // We expect that 100 different keys will distribute across all 4 partitions
        partitionsUsed.Should().OnlyContain(x => x == true);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ForKey_InvalidPartitionCount_ThrowsArgumentOutOfRangeException()
    {
        var key = new byte[] { 1 };
        Action actZero = () => PartitionRouter.ForKey(key, 0);
        Action actNegative = () => PartitionRouter.ForKey(key, -1);

        actZero.Should().Throw<ArgumentOutOfRangeException>();
        actNegative.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobin_CyclesCorrectly()
    {
        int partitionCount = 3;

        PartitionRouter.RoundRobin(0, partitionCount).Should().Be(0);
        PartitionRouter.RoundRobin(1, partitionCount).Should().Be(1);
        PartitionRouter.RoundRobin(2, partitionCount).Should().Be(2);
        PartitionRouter.RoundRobin(3, partitionCount).Should().Be(0);
        PartitionRouter.RoundRobin(4, partitionCount).Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobin_NegativeCounter_ReturnsValidPartition()
    {
        int partitionCount = 4;

        PartitionRouter.RoundRobin(-1, partitionCount).Should().BeInRange(0, partitionCount - 1);
        PartitionRouter.RoundRobin(long.MinValue, partitionCount).Should().BeInRange(0, partitionCount - 1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RoundRobin_InvalidPartitionCount_ThrowsArgumentOutOfRangeException()
    {
        Action actZero = () => PartitionRouter.RoundRobin(0, 0);
        Action actNegative = () => PartitionRouter.RoundRobin(0, -1);

        actZero.Should().Throw<ArgumentOutOfRangeException>();
        actNegative.Should().Throw<ArgumentOutOfRangeException>();
    }
}
