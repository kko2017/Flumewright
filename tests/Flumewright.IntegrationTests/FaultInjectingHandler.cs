using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Flumewright.IntegrationTests;

public class FaultInjectingHandler : DelegatingHandler
{
    private readonly string _targetPath;
    private readonly Exception _exceptionToThrow;
    private int _callCount;
    private readonly int _failAfterCount;

    public FaultInjectingHandler(HttpMessageHandler innerHandler, string targetPath, Exception exceptionToThrow, int failAfterCount = 0)
        : base(innerHandler)
    {
        _targetPath = targetPath;
        _exceptionToThrow = exceptionToThrow;
        _failAfterCount = failAfterCount;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri?.AbsolutePath.EndsWith(_targetPath) == true)
        {
            if (Interlocked.Increment(ref _callCount) > _failAfterCount)
            {
                throw _exceptionToThrow;
            }
        }
        return base.SendAsync(request, cancellationToken);
    }
}
