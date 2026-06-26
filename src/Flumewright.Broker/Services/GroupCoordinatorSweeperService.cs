using Flumewright.Broker.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Flumewright.Broker.Services;

internal class GroupCoordinatorSweeperService : BackgroundService
{
    private readonly IGroupCoordinator _coordinator;
    private readonly ILogger<GroupCoordinatorSweeperService> _logger;
    private readonly TimeSpan _sessionTimeout = TimeSpan.FromSeconds(10);
    private readonly TimeSpan _sweepInterval = TimeSpan.FromSeconds(2);

    public GroupCoordinatorSweeperService(IGroupCoordinator coordinator, ILogger<GroupCoordinatorSweeperService> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
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
        catch (Exception ex)
        {
            // Do not swallow exceptions (FIX-015 discipline)
            _logger.LogCritical(ex, "Sweeper loop crashed.");
            throw;
        }
    }
}
