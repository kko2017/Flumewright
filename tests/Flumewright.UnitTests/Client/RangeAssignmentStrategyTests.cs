using FluentAssertions;
using Flumewright.Client;
using Flumewright.Protocol;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Flumewright.UnitTests.Client;

public class RangeAssignmentStrategyTests
{
    [Fact]
    public void Assign_DistributesPartitionsEvenly()
    {
        var strategy = new RangeAssignmentStrategy();
        
        var members = new List<MemberMetadata>
        {
            new MemberMetadata { MemberId = "C1" },
            new MemberMetadata { MemberId = "C2" },
            new MemberMetadata { MemberId = "C3" }
        };
        foreach (var m in members) m.Topics.Add("topic-A");

        var partitionCounts = new Dictionary<string, int>
        {
            { "topic-A", 4 }
        };

        var assignments = strategy.Assign(members, partitionCounts);

        assignments.Should().HaveCount(3);
        
        var c1 = assignments.Single(a => a.MemberId == "C1");
        c1.Partitions.Should().BeEquivalentTo(new[] { 0, 1 }); // 2 partitions

        var c2 = assignments.Single(a => a.MemberId == "C2");
        c2.Partitions.Should().BeEquivalentTo(new[] { 2 }); // 1 partition

        var c3 = assignments.Single(a => a.MemberId == "C3");
        c3.Partitions.Should().BeEquivalentTo(new[] { 3 }); // 1 partition
    }

    [Fact]
    public void Assign_DoesNotAssignIfNoTopicMatch()
    {
        var strategy = new RangeAssignmentStrategy();
        
        var members = new List<MemberMetadata>
        {
            new MemberMetadata { MemberId = "C1" }
        };
        members[0].Topics.Add("topic-X");

        var partitionCounts = new Dictionary<string, int>
        {
            { "topic-Y", 4 }
        };

        var assignments = strategy.Assign(members, partitionCounts);

        assignments.Should().BeEmpty();
    }
}
