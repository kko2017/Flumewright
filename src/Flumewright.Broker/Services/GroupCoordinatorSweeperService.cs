using Flumewright.Broker.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Flumewright.Broker.Services;

internal class GroupCoordinatorSweeperService : BackgroundService
{
    private readonly IGroupCoordinator _coordinator;
    private readonly ILogger<GroupCoordinatorSweeperService> _logger;
    private readonly TimeSpan _sessionTimeout;
    private readonly TimeSpan _sweepInterval;

    public GroupCoordinatorSweeperService(IGroupCoordinator coordinator, ILogger<GroupCoordinatorSweeperService> logger, Microsoft.Extensions.Configuration.IConfiguration configuration)
    {
        _coordinator = coordinator;
        _logger = logger;
        _sessionTimeout = TimeSpan.FromSeconds(configuration.GetValue<double>("Broker:SessionTimeoutSeconds", 10.0));
        _sweepInterval = TimeSpan.FromSeconds(configuration.GetValue<double>("Broker:SweepIntervalSeconds", 2.0));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Group coordinator sweeper started.");
        
        using var timer = new PeriodicTimer(_sweepInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                _coordinator.SweepDeadMembers(_sessionTimeout);
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
#pragma warning disable S2139 // intentional: log + rethrow so a background liveness-service crash surfaces to the host (FIX-015, no silent swallow)
        catch (Exception ex)
        {
            // Do not swallow exceptions (FIX-015 discipline)
            _logger.LogCritical(ex, "Sweeper loop crashed.");
            throw;
        }
#pragma warning restore S2139
    }
}
