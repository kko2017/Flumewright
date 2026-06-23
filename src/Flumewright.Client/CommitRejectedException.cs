using System;

namespace Flumewright.Client;

public class CommitRejectedException : Exception
{
    public CommitRejectedException(string reason)
        : base($"Commit rejected: {reason}")
    {
    }
}
