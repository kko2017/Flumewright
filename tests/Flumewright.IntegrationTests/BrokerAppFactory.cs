using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

namespace Flumewright.IntegrationTests;

public sealed class BrokerAppFactory : IAsyncLifetime
{
    private WebApplication? _app;
    public string Address { get; private set; } = "";
    public Flumewright.Broker.Core.ITopicStore Store => _app!.Services.GetRequiredService<Flumewright.Broker.Core.ITopicStore>();

    public async Task InitializeAsync()
    {
        // Run the same bootstrap as Program.cs on port 0 -> OS allocates an empty port
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["Broker:Port"] = "0";
        builder.Configuration["Broker:PartitionsPerTopic"] = "4";

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0, listen =>     // 127.0.0.1:0 — dynamic port is OK
                    listen.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<Flumewright.Broker.Core.ITopicStore,
                                      Flumewright.Broker.Core.InMemoryTopicStore>();
        builder.Services.AddSingleton<Flumewright.Broker.Core.ICommittedOffsetStore,
                                      Flumewright.Broker.Core.InMemoryCommittedOffsetStore>();
        builder.Services.AddSingleton<Flumewright.Broker.Core.IGroupCoordinator,
                                      Flumewright.Broker.Core.GroupCoordinator>();
        builder.Services.AddHostedService<Flumewright.Broker.Services.GroupCoordinatorSweeperService>();

        _app = builder.Build();
        _app.MapGrpcService<Flumewright.Broker.Services.MessageBusService>();

        await _app.StartAsync();

        // Read the actual bound address (e.g. http://127.0.0.1:53124)
        var server = _app.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>()!;
        var bound = addresses.Addresses.First();              // e.g. http://0.0.0.0:53124
        Address = bound.Replace("0.0.0.0", "127.0.0.1")
                       .Replace("[::]", "127.0.0.1");          // Convert to an address the client can connect to
    }

    public async Task DisposeAsync()
    {
        if (_app is not null) await _app.DisposeAsync();
    }
}
