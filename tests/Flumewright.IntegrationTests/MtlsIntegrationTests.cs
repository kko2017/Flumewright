using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;
using Flumewright.Protocol;
using Flumewright.Client;
using System.Security.Cryptography.X509Certificates;

namespace Flumewright.IntegrationTests;

public class MtlsIntegrationTests : IClassFixture<MtlsBrokerAppFactory>
{
    private readonly MtlsBrokerAppFactory _factory;

    public MtlsIntegrationTests(MtlsBrokerAppFactory factory)
    {
        _factory = factory;
    }

    private GrpcChannel CreateChannel(X509Certificate2? clientCert)
    {
        var handler = new SocketsHttpHandler
        {
            // The broker CA certificate is generated dynamically, so we must tell the client to trust it.
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (sender, cert, chain, errors) =>
                {
                    if (cert == null) return false;
                    using var customChain = new X509Chain();
                    // [suppress: local test CA publishes no CRL/OCSP]
                    customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                    customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                    customChain.ChainPolicy.CustomTrustStore.Add(_factory.CaCert);
                    
                    using var certToBuild = new X509Certificate2(cert);
                    return customChain.Build(certToBuild);
                }
            }
        };

        if (clientCert != null)
        {
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { clientCert };
        }

        return GrpcChannel.ForAddress(_factory.Address, new GrpcChannelOptions { HttpHandler = handler });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Accept_WithValidClientCert_Succeeds()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var channel = CreateChannel(_factory.ValidClientCert);
        var client = new MessageBus.MessageBusClient(channel);
        
        var response = await client.PublishAsync(new PublishEnvelope
        {
            Topic = "mtls-test",
            Payload = Google.Protobuf.ByteString.CopyFromUtf8("hello")
        }, cancellationToken: cts.Token);
        
        response.Accepted.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reject_NoClientCert_Fails()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var channel = CreateChannel(null);
        var client = new MessageBus.MessageBusClient(channel);
        
        var act = async () => await client.PublishAsync(new PublishEnvelope
        {
            Topic = "mtls-test",
            Payload = Google.Protobuf.ByteString.CopyFromUtf8("hello")
        }, cancellationToken: cts.Token);
        
        var ex = await act.Should().ThrowAsync<RpcException>();
        
        Exception? current = ex.Which;
        bool foundTransportError = false;
        while (current != null)
        {
            if (current is System.IO.IOException || current is System.Security.Authentication.AuthenticationException)
            {
                foundTransportError = true;
                break;
            }
            current = current.InnerException;
        }
        foundTransportError.Should().BeTrue("Handshake failure should manifest as a transport-level IOException or AuthenticationException");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reject_UntrustedClientCert_Fails()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var channel = CreateChannel(_factory.UntrustedClientCert);
        var client = new MessageBus.MessageBusClient(channel);
        
        var act = async () => await client.PublishAsync(new PublishEnvelope
        {
            Topic = "mtls-test",
            Payload = Google.Protobuf.ByteString.CopyFromUtf8("hello")
        }, cancellationToken: cts.Token);
        
        var ex = await act.Should().ThrowAsync<RpcException>();
        
        Exception? current = ex.Which;
        bool foundTransportError = false;
        while (current != null)
        {
            if (current is System.IO.IOException || current is System.Security.Authentication.AuthenticationException)
            {
                foundTransportError = true;
                break;
            }
            current = current.InnerException;
        }
        foundTransportError.Should().BeTrue("Handshake failure should manifest as a transport-level IOException or AuthenticationException");
    }
}
