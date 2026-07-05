using Flumewright.Broker.Core;
using Flumewright.Broker.Services;
using Flumewright.Broker.Configuration;
using Flumewright.Broker.Interceptors;
using Flumewright.Security.Cryptography;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using System.Security.Cryptography.X509Certificates;

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
                ClientCertificateValidation = (cert, chain, errors) => ClientCertificateValidator.Validate(cert, caCert)
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
