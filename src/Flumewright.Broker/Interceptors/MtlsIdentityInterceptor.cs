using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Flumewright.Security.Identity;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Flumewright.Broker.Interceptors;

public class MtlsIdentityInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ExtractIdentity(context);
        return await continuation(request, context);
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ExtractIdentity(context);
        return await continuation(requestStream, context);
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ExtractIdentity(context);
        await continuation(request, responseStream, context);
    }

    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ExtractIdentity(context);
        await continuation(requestStream, responseStream, context);
    }

    private static void ExtractIdentity(ServerCallContext context)
    {
        var httpContext = context.GetHttpContext();
        var clientCert = httpContext.Connection.ClientCertificate;

        if (clientCert == null)
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Client certificate is missing."));
        }

        var cn = clientCert.GetNameInfo(X509NameType.SimpleName, forIssuer: false);
        if (string.IsNullOrWhiteSpace(cn))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Client certificate does not have a valid Common Name (CN)."));
        }

        context.UserState[IdentityConstants.PrincipalContextKey] = $"{IdentityConstants.PrincipalPrefix}{cn}";
    }
}
