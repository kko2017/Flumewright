using System;

namespace Flumewright.Client.Resilience;

/// <summary>
/// An exception thrown by a message handler to indicate that the message is a "poison pill"
/// (e.g. malformed payload) and should NOT be retried, but sent directly to the DLQ.
/// </summary>
public class PoisonMessageException : Exception
{
    public PoisonMessageException(string message) : base(message)
    {
    }

    public PoisonMessageException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
