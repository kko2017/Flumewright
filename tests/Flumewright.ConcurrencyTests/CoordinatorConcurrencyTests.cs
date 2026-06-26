using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Flumewright.Broker.Core;
using Flumewright.Protocol;
using Microsoft.Coyote.SystematicTesting;
using Xunit;

namespace Flumewright.ConcurrencyTests
{
    public class CoordinatorConcurrencyTests
    {
        [Microsoft.Coyote.SystematicTesting.Test]
        internal static async Task TestConcurrentJoinAndLeave()
        {
            var coordinator = new GroupCoordinator();
            string groupId = "test-group";
            var topics = new[] { "topic1" };

            GroupJoinResult? r1 = null;
            GroupJoinResult? r2 = null;

            var t1 = Task.Run(async () => {
                try {
                    // Use a very long timeout so we don't rely on real time
                    r1 = await coordinator.JoinGroupAsync(groupId, "m1", topics, TimeSpan.FromHours(1), CancellationToken.None);
                } catch (InvalidOperationException) { }
            });
            
            var t2 = Task.Run(async () => {
                try {
                    r2 = await coordinator.JoinGroupAsync(groupId, "m2", topics, TimeSpan.FromHours(1), CancellationToken.None);
                } catch (InvalidOperationException) { }
            });

            var t3 = Task.Run(() => {
                coordinator.RemoveMember(groupId, "m1");
            });

            var t4 = Task.Run(() => {
                coordinator.RemoveMember(groupId, "m2");
            });

            await Task.WhenAll(t1, t2, t3, t4);

            var finalGen = coordinator.GetGroupGeneration(groupId);
            Assert.True(finalGen != null, "Generation should not be null after joins");

            if (r1 != null && r1.IsLeader)
            {
                Assert.Contains(r1.Members, m => m.MemberId == "m1");
                if (r2 != null && r2.Generation == r1.Generation)
                {
                    Assert.Contains(r1.Members, m => m.MemberId == "m2");
                }
            }

            if (r2 != null && r2.IsLeader)
            {
                Assert.Contains(r2.Members, m => m.MemberId == "m2");
                if (r1 != null && r1.Generation == r2.Generation)
                {
                    Assert.Contains(r1.Members, m => m.MemberId == "m1");
                }
            }
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        internal static async Task TestSweeperRacingHeartbeat()
        {
            var coordinator = new GroupCoordinator();
            string groupId = "test-group";
            
            var r1 = await coordinator.JoinGroupAsync(groupId, "m1", new[] { "t1" }, TimeSpan.FromHours(1), CancellationToken.None);
            int gen = r1.Generation;

            GroupErrorCode hbResult = GroupErrorCode.GroupUnknownMember;
            
            var t1 = Task.Run(() => {
                hbResult = coordinator.RecordHeartbeat(groupId, "m1", gen);
            });

            var t2 = Task.Run(() => {
                coordinator.SweepDeadMembers(TimeSpan.FromMilliseconds(-1));
            });

            await Task.WhenAll(t1, t2);

            var finalState = coordinator.GetGroupState(groupId);
            bool isEvicted = finalState!.Members.All(m => m.MemberId != "m1");
            
            if (hbResult == GroupErrorCode.GroupOk)
            {
                if (isEvicted) Assert.True(finalState.Generation > gen);
                else Assert.Equal(gen, finalState.Generation);
            }
            else if (hbResult == GroupErrorCode.GroupUnknownMember)
            {
                Assert.True(isEvicted);
                Assert.True(finalState.Generation > gen);
            }
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        internal static async Task TestGenerationFence()
        {
            var coordinator = new GroupCoordinator();
            string groupId = "test-group";
            
            var r1 = await coordinator.JoinGroupAsync(groupId, "m1", new[] { "t1" }, TimeSpan.FromHours(1), CancellationToken.None);
            int oldGen = r1.Generation;

            GroupErrorCode hbResult = GroupErrorCode.GroupOk;
            
            var t1 = Task.Run(() => {
                hbResult = coordinator.RecordHeartbeat(groupId, "m1", oldGen);
            });

            using var cts = new CancellationTokenSource();

            var t2 = Task.Run(async () => {
                try {
                    await coordinator.JoinGroupAsync(groupId, "m2", new[] { "t1" }, TimeSpan.FromHours(1), cts.Token);
                } catch (InvalidOperationException) { }
            });

            var t3 = Task.Run(() => {
                // Cancel m2's join to unblock it and simulate timeout
                cts.Cancel();
            });

            await Task.WhenAll(t1, t2, t3);

            var finalGen = coordinator.GetGroupGeneration(groupId);
            Assert.True(finalGen > oldGen, "Generation should have bumped due to m2 joining");
            
            Assert.True(
                hbResult == GroupErrorCode.GroupOk || 
                hbResult == GroupErrorCode.GroupRebalanceInProgress || 
                hbResult == GroupErrorCode.GroupFenced,
                $"Unexpected heartbeat result: {hbResult}"
            );
        }

        [Fact]
        public void RunTestConcurrentJoinAndLeave()
        {
            RunSystematicTest(TestConcurrentJoinAndLeave);
        }

        [Fact]
        public void RunTestSweeperRacingHeartbeat()
        {
            RunSystematicTest(TestSweeperRacingHeartbeat);
        }

        [Fact]
        public void RunTestGenerationFence()
        {
            RunSystematicTest(TestGenerationFence);
        }

        private static void RunSystematicTest(Func<Task> test)
        {
            var config = Microsoft.Coyote.Configuration.Create()
                .WithTestingIterations(100)
                .WithMaxSchedulingSteps(300);

            var engine = TestingEngine.Create(config, test);
            engine.Run();

            if (engine.TestReport.NumOfFoundBugs > 0)
            {
                var bugs = string.Join(Environment.NewLine, engine.TestReport.BugReports);
                Assert.Fail($"Coyote found {engine.TestReport.NumOfFoundBugs} bugs: {bugs}");
            }
        }
    }
}
