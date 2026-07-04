using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Broker.Interceptors;
using Flumewright.Security.Identity;
using Grpc.Core;
using Grpc.Core.Testing;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace Flumewright.UnitTests;

public class MtlsIdentityInterceptorTests
{
    private readonly MtlsIdentityInterceptor _interceptor;

    public MtlsIdentityInterceptorTests()
    {
        _interceptor = new MtlsIdentityInterceptor();
    }

    private static ServerCallContext CreateContext(X509Certificate2? clientCert)
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.ClientCertificate = clientCert;

        var context = TestServerCallContext.Create(
            method: "Test",
            host: "localhost",
            deadline: DateTime.UtcNow.AddMinutes(1),
            requestHeaders: new Metadata(),
            cancellationToken: CancellationToken.None,
            peer: "ipv4:127.0.0.1:1234",
            authContext: null!,
            contextPropagationToken: null,
            writeHeadersFunc: (meta) => Task.CompletedTask,
            writeOptionsGetter: () => new WriteOptions(),
            writeOptionsSetter: (options) => { }
        );
        
        context.UserState["__HttpContext"] = httpContext;
        return context;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Handler_WithNullHttpContext_ThrowsUnauthenticated()
    {
        var context = TestServerCallContext.Create(
            method: "Test",
            host: "localhost",
            deadline: DateTime.UtcNow.AddMinutes(1),
            requestHeaders: new Metadata(),
            cancellationToken: CancellationToken.None,
            peer: "ipv4:127.0.0.1:1234",
            authContext: null!,
            contextPropagationToken: null,
            writeHeadersFunc: (meta) => Task.CompletedTask,
            writeOptionsGetter: () => new WriteOptions(),
            writeOptionsSetter: (options) => { }
        );

        var act = () => _interceptor.UnaryServerHandler("request", context, (req, ctx) => Task.FromResult("response"));

        var ex = await act.Should().ThrowAsync<RpcException>();
        ex.Which.StatusCode.Should().Be(StatusCode.Unauthenticated);
        ex.Which.Status.Detail.Should().Contain("HTTP context is missing");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UnaryServerHandler_WithValidCert_SetsPrincipalOnContext()
    {
        // Using X509Certificate2 from BCL to quickly generate a self-signed cert for testing
        using var rsa = System.Security.Cryptography.RSA.Create();
        var req = new CertificateRequest("CN=alice", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));

        var context = CreateContext(cert);

        await _interceptor.UnaryServerHandler("request", context, (req, ctx) => Task.FromResult("response"));

        context.UserState.Should().ContainKey(IdentityConstants.PrincipalContextKey);
        context.UserState[IdentityConstants.PrincipalContextKey].Should().Be("User:alice");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UnaryServerHandler_NoClientCert_ThrowsUnauthenticated()
    {
        var context = CreateContext(null);

        var act = () => _interceptor.UnaryServerHandler("request", context, (req, ctx) => Task.FromResult("response"));

        var ex = await act.Should().ThrowAsync<RpcException>();
        ex.Which.StatusCode.Should().Be(StatusCode.Unauthenticated);
        ex.Which.Status.Detail.Should().Contain("Client certificate is missing");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task UnaryServerHandler_CertWithoutCN_ThrowsUnauthenticated()
    {
        // Create a cert without a Common Name (CN)
        using var rsa = System.Security.Cryptography.RSA.Create();
        var req = new CertificateRequest("O=NoCommonName", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));

        var context = CreateContext(cert);

        var act = () => _interceptor.UnaryServerHandler("request", context, (req, ctx) => Task.FromResult("response"));

        var ex = await act.Should().ThrowAsync<RpcException>();
        ex.Which.StatusCode.Should().Be(StatusCode.Unauthenticated);
        ex.Which.Status.Detail.Should().Contain("Common Name (CN)");
    }
    [Fact]
    [Trait("Category", "Unit")]
    public async Task ClientStreamingServerHandler_WithValidCert_SetsPrincipalOnContext()
    {
        using var rsa = System.Security.Cryptography.RSA.Create();
        var req = new CertificateRequest("CN=bob", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));

        var context = CreateContext(cert);

        // For IAsyncStreamReader we can just mock or pass null since the handler doesn't use it directly
        await _interceptor.ClientStreamingServerHandler<string, string>(null!, context, (reqStream, ctx) => Task.FromResult("response"));

        context.UserState.Should().ContainKey(IdentityConstants.PrincipalContextKey);
        context.UserState[IdentityConstants.PrincipalContextKey].Should().Be("User:bob");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ServerStreamingServerHandler_WithValidCert_SetsPrincipalOnContext()
    {
        using var rsa = System.Security.Cryptography.RSA.Create();
        var req = new CertificateRequest("CN=charlie", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));

        var context = CreateContext(cert);

        await _interceptor.ServerStreamingServerHandler<string, string>("request", null!, context, (req, respStream, ctx) => Task.CompletedTask);

        context.UserState.Should().ContainKey(IdentityConstants.PrincipalContextKey);
        context.UserState[IdentityConstants.PrincipalContextKey].Should().Be("User:charlie");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DuplexStreamingServerHandler_WithValidCert_SetsPrincipalOnContext()
    {
        using var rsa = System.Security.Cryptography.RSA.Create();
        var req = new CertificateRequest("CN=dave", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));

        var context = CreateContext(cert);

        await _interceptor.DuplexStreamingServerHandler<string, string>(null!, null!, context, (reqStream, respStream, ctx) => Task.CompletedTask);

        context.UserState.Should().ContainKey(IdentityConstants.PrincipalContextKey);
        context.UserState[IdentityConstants.PrincipalContextKey].Should().Be("User:dave");
    }
}
