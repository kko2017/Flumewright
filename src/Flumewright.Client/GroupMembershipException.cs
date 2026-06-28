using System;

namespace Flumewright.Client;

public class GroupMembershipException : Exception
{
    public ClientGroupErrorCode? Code { get; }

    public GroupMembershipException(string message)
        : base(message)
    {
    }

    public GroupMembershipException(string message, ClientGroupErrorCode code)
        : base(message)
    {
        Code = code;
    }
}
