using Flumewright.Broker.Core;
using Flumewright.Broker.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// M1: plaintext HTTP/2 (h2c). mTLS/HTTPS is M4.
builder.WebHost.ConfigureKestrel(options =>
{
    var port = builder.Configuration.GetValue<int>("Broker:Port", 5050);
    options.ListenAnyIP(port, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Services.AddGrpc();
builder.Services.AddSingleton<ITopicStore, InMemoryTopicStore>();

var app = builder.Build();

app.MapGrpcService<MessageBusService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client.");

await app.RunAsync();

public partial class Program
{
    private Program() { }
}
