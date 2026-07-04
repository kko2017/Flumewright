using Flumewright.Broker.Core;
using Flumewright.Broker.Services;
using Flumewright.Broker.Interceptors;
using Microsoft.AspNetCore.Server.Kestrel.Core;

using Flumewright.Broker.Configuration;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Server.Kestrel.Https;

var builder = WebApplication.CreateBuilder(args);

var mtlsConfig = MtlsConfig.FromConfiguration(builder.Configuration);
var port = builder.Configuration.GetValue<int>("Broker:Port", 5050);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(port, listen =>
    {
        listen.Protocols = HttpProtocols.Http2;

        if (mtlsConfig.RequireClientCertificate)
        {
            var serverCert = new X509Certificate2(mtlsConfig.ServerCertPath!);
            var caCert = new X509Certificate2(mtlsConfig.CaCertPath!);

            listen.UseHttps(new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (cert, chain, errors) =>
                {
                    if (cert == null) return false;

                    using var customChain = new X509Chain();
                    customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                    customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                    customChain.ChainPolicy.CustomTrustStore.Add(caCert);

                    return customChain.Build(cert);
                }
            });
        }
    });
});

builder.Services.AddGrpc(options =>
{
    if (mtlsConfig.RequireClientCertificate)
    {
        options.Interceptors.Add<MtlsIdentityInterceptor>();
    }
});
builder.Services.AddSingleton<ITopicStore, InMemoryTopicStore>();
builder.Services.AddSingleton<ICommittedOffsetStore, InMemoryCommittedOffsetStore>();
builder.Services.AddSingleton<IGroupCoordinator, GroupCoordinator>();
builder.Services.AddHostedService<GroupCoordinatorSweeperService>();

var app = builder.Build();

app.MapGrpcService<MessageBusService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client.");

await app.RunAsync();

public partial class Program
{
    private Program() { }
}
