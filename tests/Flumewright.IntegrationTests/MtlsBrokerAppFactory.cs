using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Flumewright.Broker.Configuration;
using Flumewright.Broker.Core;
using Flumewright.Broker.Interceptors;
using Flumewright.Broker.Services;
using Flumewright.CertGen;
using Flumewright.Security.Cryptography;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Flumewright.IntegrationTests;

public sealed class MtlsBrokerAppFactory : IAsyncLifetime
{
    private WebApplication? _app;
    private string _tempDir = "";
    
    public string Address { get; private set; } = "";
    public GeneratedCertificates? Certs { get; private set; }
    private X509Certificate2? _serverCert;
    private X509Certificate2? _caCert;

    public X509Certificate2 ValidClientCert => Certs!.Clients[0];
    public X509Certificate2 UntrustedClientCert => Certs!.Untrusted;
    public X509Certificate2 CaCert => Certs!.Ca;
    public ITopicStore Store => _app!.Services.GetRequiredService<ITopicStore>();

    private static readonly string[] ClientNames = new[] { "alice" };

    public async Task InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "fw-mtls-test-" + Guid.NewGuid().ToString("N"));
        Certs = CertificateGenerator.GenerateAll(_tempDir, ClientNames);

        var builder = WebApplication.CreateBuilder();
        builder.Configuration["Broker:Port"] = "0";
        builder.Configuration["Broker:PartitionsPerTopic"] = "4";
        builder.Configuration["Broker:SessionTimeoutSeconds"] = "1";
        builder.Configuration["Broker:SweepIntervalSeconds"] = "0.25";
        
        builder.Configuration["Broker:RequireClientCertificate"] = "true";
        builder.Configuration["Broker:ServerCertPath"] = Path.Combine(_tempDir, "broker.pfx");
        builder.Configuration["Broker:CaCertPath"] = Path.Combine(_tempDir, "ca.pfx");

        var mtlsConfig = MtlsConfig.FromConfiguration(builder.Configuration);

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0, listen =>
            {
                listen.Protocols = HttpProtocols.Http2;

                _serverCert = new X509Certificate2(mtlsConfig.ServerCertPath!);
                _caCert = new X509Certificate2(mtlsConfig.CaCertPath!);

                listen.UseHttps(new HttpsConnectionAdapterOptions
                {
                    ServerCertificate = _serverCert,
                    ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                    ClientCertificateValidation = (cert, chain, errors) => ClientCertificateValidator.Validate(cert, _caCert)
                });
            });
        });

        builder.Services.AddGrpc(options =>
        {
            options.Interceptors.Add<MtlsIdentityInterceptor>();
        });
        
        builder.Services.AddSingleton<ITopicStore, InMemoryTopicStore>();
        builder.Services.AddSingleton<ICommittedOffsetStore, InMemoryCommittedOffsetStore>();
        builder.Services.AddSingleton<IGroupCoordinator, GroupCoordinator>();
        builder.Services.AddHostedService<GroupCoordinatorSweeperService>();

        _app = builder.Build();
        _app.MapGrpcService<MessageBusService>();

        await _app.StartAsync();

        var server = _app.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>()!;
        var bound = addresses.Addresses.First();
        
        // Ensure https scheme
        Address = bound.Replace("0.0.0.0", "127.0.0.1")
                       .Replace("[::]", "127.0.0.1")
                       .Replace("http://", "https://");
    }

    public async Task DisposeAsync()
    {
        if (_app is not null) await _app.DisposeAsync();
        
        _serverCert?.Dispose();
        _caCert?.Dispose();
        Certs?.Dispose();
        
        if (Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, true);
            }
            catch { // [suppress: cleanup best-effort] 
            }
        }
    }
}
