namespace Flumewright.Broker.Core;

internal static class GroupMessages
{
    public const string RebalanceInProgress = "Rebalance in progress";
    public const string GroupNotFound = "Group not found";
    public const string Fenced = "Fenced";
    public const string MemberNotFound = "Member not found";

    public const string OffsetCannotBeNegative = "Offset cannot be negative";
    public const string FencedStaleGeneration = "Fenced: stale generation";
    public const string UnknownTopicOrInvalidPartition = "Unknown topic or invalid partition";
    public const string OffsetOutOfRange = "Offset out of range";
    public const string BackwardsCommitRejected = "Backwards commit rejected";
}
