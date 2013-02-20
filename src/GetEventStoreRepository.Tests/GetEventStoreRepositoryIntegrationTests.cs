using System;
using System.Net;
using System.Text;
using CommonDomain;
using CommonDomain.Persistence;
using EventStore.ClientAPI;
using NUnit.Framework;
using Newtonsoft.Json.Linq;

namespace GetEventStoreRepository.Tests
{
    /// <summary>
    /// Integration tests for the GetEventStoreRepository. These tests require a
    /// running version of the Event Store, with a TCP endpoint as specified in the
    /// IntegrationTestTcpEndPoint field (defaults to local loopback, port 1113).
    /// </summary>
    [TestFixture]
    public class GetEventStoreRepositoryIntegrationTests
    {
        /// <summary>
        /// Set this to the TCP endpoint on which the Event Store is running.
        /// </summary>
        private static readonly IPEndPoint IntegrationTestTcpEndPoint = new IPEndPoint(IPAddress.Loopback, 1113);

        private static Guid SaveTestAggregateWithoutCustomHeaders(IRepository repository, int numberOfEvents)
        {
            var aggregateToSave = new TestAggregate(Guid.NewGuid());
            aggregateToSave.ProduceEvents(numberOfEvents);
            repository.Save(aggregateToSave, Guid.NewGuid(), d => { });
            return aggregateToSave.Id;
        }

        [Test]
        public void CanGetLatestVersionById()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var savedId = SaveTestAggregateWithoutCustomHeaders(repo, 3000 /* excludes TestAggregateCreated */);

            var retrieved = repo.GetById<TestAggregate>(savedId);
            Assert.AreEqual(3000, retrieved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void CanGetSpecificVersionFromFirstPageById()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var savedId = SaveTestAggregateWithoutCustomHeaders(repo, 100 /* excludes TestAggregateCreated */);

            var retrieved = repo.GetById<TestAggregate>(savedId, 65);
            Assert.AreEqual(64, retrieved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void CanGetSpecificVersionFromSubsequentPageById()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var savedId = SaveTestAggregateWithoutCustomHeaders(repo, 500 /* excludes TestAggregateCreated */);

            var retrieved = repo.GetById<TestAggregate>(savedId, 126);
            Assert.AreEqual(125, retrieved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void CanHandleLargeNumberOfEventsInOneTransaction()
        {
            const int numberOfEvents = 50000;

            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var aggregateId = SaveTestAggregateWithoutCustomHeaders(repo, numberOfEvents);

            var saved = repo.GetById<TestAggregate>(aggregateId);
            Assert.AreEqual(numberOfEvents, saved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void CanSaveExistingAggregate()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var savedId = SaveTestAggregateWithoutCustomHeaders(repo, 100 /* excludes TestAggregateCreated */);

            var firstSaved = repo.GetById<TestAggregate>(savedId);
            firstSaved.ProduceEvents(50);
            repo.Save(firstSaved, Guid.NewGuid(), d => { });

            var secondSaved = repo.GetById<TestAggregate>(savedId);
            Assert.AreEqual(150, secondSaved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void CanSaveMultiplesOfWritePageSize()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var savedId = SaveTestAggregateWithoutCustomHeaders(repo, 1500 /* excludes TestAggregateCreated */);
            var saved = repo.GetById<TestAggregate>(savedId);

            Assert.AreEqual(1500, saved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void ClearsEventsFromAggregateOnceCommitted()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var aggregateToSave = new TestAggregate(Guid.NewGuid());
            aggregateToSave.ProduceEvents(10);
            repo.Save(aggregateToSave, Guid.NewGuid(), d => { });

            Assert.AreEqual(0, ((IAggregate) aggregateToSave).GetUncommittedEvents().Count);
        }

        [Test]
        public void GetsEventsFromCorrectStreams()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var aggregate1Id = SaveTestAggregateWithoutCustomHeaders(repo, 100);
            var aggregate2Id = SaveTestAggregateWithoutCustomHeaders(repo, 50);

            var firstSaved = repo.GetById<TestAggregate>(aggregate1Id);
            Assert.AreEqual(100, firstSaved.AppliedEventCount);

            var secondSaved = repo.GetById<TestAggregate>(aggregate2Id);
            Assert.AreEqual(50, secondSaved.AppliedEventCount);

            connection.Close();
        }

        [Test]
        public void SavesCommitHeadersOnEachEvent()
        {
            var connection = EventStoreConnection.Create();
            connection.Connect(IntegrationTestTcpEndPoint);
            var repo = new GetEventStoreRepository(connection);

            var commitId = Guid.NewGuid();
            var aggregateToSave = new TestAggregate(Guid.NewGuid());
            aggregateToSave.ProduceEvents(20);
            repo.Save(aggregateToSave, commitId, d => {
                d.Add("CustomHeader1", "CustomValue1");
                d.Add("CustomHeader2", "CustomValue2");
            });

            var read = connection.ReadStreamEventsForward(string.Format("aggregate-{0}", aggregateToSave.Id), 1, 20, false);
            foreach (var serializedEvent in read.Events)
            {
                var parsedMetadata = JObject.Parse(Encoding.UTF8.GetString(serializedEvent.OriginalEvent.Metadata));
                var deserializedCommitId = parsedMetadata.Property("CommitId").Value.ToObject<Guid>();
                Assert.AreEqual(commitId, deserializedCommitId);

                var deserializedCustomHeader1 = parsedMetadata.Property("CustomHeader1").Value.ToObject<string>();
                Assert.AreEqual("CustomValue1", deserializedCustomHeader1);

                var deserializedCustomHeader2 = parsedMetadata.Property("CustomHeader2").Value.ToObject<string>();
                Assert.AreEqual("CustomValue2", deserializedCustomHeader2);
            }

            connection.Close();
        }
    }
}