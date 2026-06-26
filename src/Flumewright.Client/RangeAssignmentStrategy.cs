using System;
using System.Collections.Generic;
using System.Linq;
using Flumewright.Protocol;

namespace Flumewright.Client;

public class RangeAssignmentStrategy : IAssignmentStrategy
{
    public string Name => "range";

    public IReadOnlyList<MemberAssignment> Assign(
        IReadOnlyList<MemberMetadata> members,
        IReadOnlyDictionary<string, int> partitionCounts)
    {
        var assignments = new List<MemberAssignment>();

        // For each topic, find the members subscribed to it, sort them, and assign ranges.
        var topics = members.SelectMany(m => m.Topics).Distinct().OrderBy(t => t).ToList();

        foreach (var topic in topics)
        {
            if (!partitionCounts.TryGetValue(topic, out var partitionCount))
            {
                continue; // Cannot assign partitions for unknown topics
            }

            var consumersForTopic = members
                .Where(m => m.Topics.Contains(topic))
                .Select(m => m.MemberId)
                .OrderBy(id => id)
                .ToList();

            if (consumersForTopic.Count == 0)
                continue;

            int numPartitionsPerConsumer = partitionCount / consumersForTopic.Count;
            int consumersWithExtraPartition = partitionCount % consumersForTopic.Count;

            int partitionIndex = 0;
            for (int i = 0; i < consumersForTopic.Count; i++)
            {
                int numPartitions = numPartitionsPerConsumer + (i < consumersWithExtraPartition ? 1 : 0);
                if (numPartitions > 0)
                {
                    var assignment = new MemberAssignment
                    {
                        MemberId = consumersForTopic[i],
                        Topic = topic
                    };
                    for (int p = 0; p < numPartitions; p++)
                    {
                        assignment.Partitions.Add(partitionIndex++);
                    }
                    assignments.Add(assignment);
                }
            }
        }

        return assignments;
    }
}
