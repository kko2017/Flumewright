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

    public async Task InitializeAsync()
    {
        // Program.cs와 동일한 부트스트랩을 포트 0으로 실행 → OS가 빈 포트 할당
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["Broker:Port"] = "0";

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0, listen =>     // 127.0.0.1:0 — 동적 포트 OK
                    listen.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();
        builder.Services.AddSingleton<Flumewright.Broker.Core.ITopicStore,
                                      Flumewright.Broker.Core.InMemoryTopicStore>();

        _app = builder.Build();
        _app.MapGrpcService<Flumewright.Broker.Services.MessageBusService>();

        await _app.StartAsync();

        // 실제 바인딩된 주소 읽기 (예: http://127.0.0.1:53124)
        var server = _app.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>()!;
        var bound = addresses.Addresses.First();              // 예: http://0.0.0.0:53124
        Address = bound.Replace("0.0.0.0", "127.0.0.1")
                       .Replace("[::]", "127.0.0.1");          // 클라이언트가 붙을 수 있는 주소로
    }

    public async Task DisposeAsync()
    {
        if (_app is not null) await _app.DisposeAsync();
    }
}
