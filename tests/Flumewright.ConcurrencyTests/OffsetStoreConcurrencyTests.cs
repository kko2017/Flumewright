using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Flumewright.Broker.Core;
using Flumewright.Protocol;
using Microsoft.Coyote.SystematicTesting;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Flumewright.ConcurrencyTests
{
    public class OffsetStoreConcurrencyTests
    {
        private class DummyTopicStore : ITopicStore
        {
            public long HighWatermark { get; set; } = 1000;

            public ValueTask<(int Partition, long Offset)> PublishAsync(string topic, ReadOnlyMemory<byte> partitionKey, IReadOnlyDictionary<string, string> headers, ReadOnlyMemory<byte> payload, CancellationToken ct = default) => throw new NotImplementedException();
            public IAsyncEnumerable<StoredMessage> SubscribeAsync(string topic, CancellationToken ct = default) => throw new NotImplementedException();
            public IAsyncEnumerable<StoredMessage> SubscribeAsync(string topic, long startOffset, CancellationToken ct = default) => throw new NotImplementedException();
            public IAsyncEnumerable<StoredMessage> ReadPartitionAsync(string topic, int partition, long startOffset, CancellationToken ct = default) => throw new NotImplementedException();
            public IAsyncEnumerable<StoredMessage> ReadPartitionsAsync(string topic, IReadOnlyDictionary<int, long> partitionOffsets, CancellationToken ct = default) => throw new NotImplementedException();
            
            public long? GetPartitionHighWatermark(string topic, int partition) => HighWatermark;
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        internal static async Task TestConcurrentCommits()
        {
            var store = new InMemoryCommittedOffsetStore(new DummyTopicStore { HighWatermark = 1000 }, null);
            string groupId = "test-group";
            string topic = "topic1";
            int partition = 0;

            var t1 = Task.Run(async () => await store.CommitOffsetAsync(groupId, topic, partition, 10));
            var t2 = Task.Run(async () => await store.CommitOffsetAsync(groupId, topic, partition, 20));
            var t3 = Task.Run(async () => await store.CommitOffsetAsync(groupId, topic, partition, 30));

            await Task.WhenAll(t1, t2, t3);

            var finalOffset = await store.GetCommittedOffsetAsync(groupId, topic, partition);
            
            Assert.Equal(30, finalOffset);
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        internal static async Task TestGenerationFenceVsCommitRace()
        {
            var coordinator = new GroupCoordinator();
            var store = new InMemoryCommittedOffsetStore(new DummyTopicStore { HighWatermark = 1000 }, coordinator);
            string groupId = "test-group";
            string topic = "topic1";
            int partition = 0;

            var joinResult = await coordinator.JoinGroupAsync(groupId, "m1", new[] { topic }, TimeSpan.FromHours(1), CancellationToken.None);
            int oldGen = joinResult.Generation;

            (bool Ok, string Reason, GroupErrorCode Code) commitResult = default;

            var t1 = Task.Run(async () => {
                commitResult = await store.CommitOffsetAsync(groupId, topic, partition, 10, oldGen);
            });

            using var cts = new CancellationTokenSource();

            var t2 = Task.Run(async () => {
                try {
                    await coordinator.JoinGroupAsync(groupId, "m2", new[] { topic }, TimeSpan.FromHours(1), cts.Token);
                } catch (InvalidOperationException) {
                    // [suppress: intended cancel-first interleaving]
                } catch (OperationCanceledException) { 
                    // [suppress: intended cancel-first interleaving]
                }
            });

            var t3 = Task.Run(() => {
                cts.Cancel();
            });

            await Task.WhenAll(t1, t2, t3);

            var finalOffset = await store.GetCommittedOffsetAsync(groupId, topic, partition);
            var finalGen = coordinator.GetGroupGeneration(groupId);

            if (commitResult.Ok)
            {
                Assert.Equal(10, finalOffset);
            }
            else
            {
                Assert.Equal(GroupErrorCode.GroupFenced, commitResult.Code);
                Assert.True(finalGen > oldGen, "Generation must be bumped if commit was fenced");
                Assert.Null(finalOffset);
            }
        }

        [Fact]
        public void RunTestConcurrentCommits()
        {
            RunSystematicTest(TestConcurrentCommits);
        }

        [Fact]
        public void RunTestGenerationFenceVsCommitRace()
        {
            RunSystematicTest(TestGenerationFenceVsCommitRace);
        }

        private static void RunSystematicTest(Func<Task> test)
        {
            var config = Microsoft.Coyote.Configuration.Create()
                .WithTestingIterations(100)
                .WithMaxSchedulingSteps(300);

            var engine = TestingEngine.Create(config, test);
            engine.Run();

            Console.WriteLine(engine.GetReport());

            if (engine.TestReport.NumOfFoundBugs > 0)
            {
                var bugs = string.Join(Environment.NewLine, engine.TestReport.BugReports);
                Assert.Fail($"Coyote found {engine.TestReport.NumOfFoundBugs} bugs: {bugs}\n\nReport:\n{engine.GetReport()}");
            }
        }
    }
}
