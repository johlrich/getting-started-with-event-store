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

        private EventStoreConnection _connection;
        private GetEventStoreRepository _repo;

        [SetUp]
        public void SetUp()
        {
            _connection = EventStoreConnection.Create();
            _connection.Connect(IntegrationTestTcpEndPoint);
            _repo = new GetEventStoreRepository(_connection);
        }

        [TearDown]
        public void TearDown()
        {
            _connection.Close();
        }

        [Test]
        public void CanGetLatestVersionById()
        {
            var savedId = SaveTestAggregateWithoutCustomHeaders(_repo, 3000 /* excludes TestAggregateCreated */);

            var retrieved = _repo.GetById<TestAggregate>(savedId);
            Assert.AreEqual(3000, retrieved.AppliedEventCount);
        }

        [Test]
        public void CanGetSpecificVersionFromFirstPageById()
        {
            var savedId = SaveTestAggregateWithoutCustomHeaders(_repo, 100 /* excludes TestAggregateCreated */);

            var retrieved = _repo.GetById<TestAggregate>(savedId, 65);
            Assert.AreEqual(64, retrieved.AppliedEventCount);
        }

        [Test]
        public void CanGetSpecificVersionFromSubsequentPageById()
        {
            var savedId = SaveTestAggregateWithoutCustomHeaders(_repo, 500 /* excludes TestAggregateCreated */);

            var retrieved = _repo.GetById<TestAggregate>(savedId, 126);
            Assert.AreEqual(125, retrieved.AppliedEventCount);
        }

        [Test]
        public void CanHandleLargeNumberOfEventsInOneTransaction()
        {
            const int numberOfEvents = 50000;

            var aggregateId = SaveTestAggregateWithoutCustomHeaders(_repo, numberOfEvents);

            var saved = _repo.GetById<TestAggregate>(aggregateId);
            Assert.AreEqual(numberOfEvents, saved.AppliedEventCount);
        }

        [Test]
        public void CanSaveExistingAggregate()
        {
            var savedId = SaveTestAggregateWithoutCustomHeaders(_repo, 100 /* excludes TestAggregateCreated */);

            var firstSaved = _repo.GetById<TestAggregate>(savedId);
            firstSaved.ProduceEvents(50);
            _repo.Save(firstSaved, Guid.NewGuid(), d => { });

            var secondSaved = _repo.GetById<TestAggregate>(savedId);
            Assert.AreEqual(150, secondSaved.AppliedEventCount);
        }

        [Test]
        public void CanSaveMultiplesOfWritePageSize()
        {
            var savedId = SaveTestAggregateWithoutCustomHeaders(_repo, 1500 /* excludes TestAggregateCreated */);
            var saved = _repo.GetById<TestAggregate>(savedId);

            Assert.AreEqual(1500, saved.AppliedEventCount);
        }

        [Test]
        public void ClearsEventsFromAggregateOnceCommitted()
        {
            var aggregateToSave = new TestAggregate(Guid.NewGuid());
            aggregateToSave.ProduceEvents(10);
            _repo.Save(aggregateToSave, Guid.NewGuid(), d => { });

            Assert.AreEqual(0, ((IAggregate) aggregateToSave).GetUncommittedEvents().Count);
        }

        [Test]
        public void ThrowsOnRequestingSpecificVersionHigherThanExists()
        {
            var aggregateId = SaveTestAggregateWithoutCustomHeaders(_repo, 10);

            Assert.Throws<AggregateVersionException>(() => _repo.GetById<TestAggregate>(aggregateId, 50));
        }

        [Test]
        public void GetsEventsFromCorrectStreams()
        {
            var aggregate1Id = SaveTestAggregateWithoutCustomHeaders(_repo, 100);
            var aggregate2Id = SaveTestAggregateWithoutCustomHeaders(_repo, 50);

            var firstSaved = _repo.GetById<TestAggregate>(aggregate1Id);
            Assert.AreEqual(100, firstSaved.AppliedEventCount);

            var secondSaved = _repo.GetById<TestAggregate>(aggregate2Id);
            Assert.AreEqual(50, secondSaved.AppliedEventCount);
        }

        [Test]
        public void ThrowsOnGetNonExistentAggregate()
        {
            Assert.Throws<AggregateNotFoundException>(() => _repo.GetById<TestAggregate>(Guid.NewGuid()));
        }

        [Test]
        public void ThrowsOnGetDeletedAggregate()
        {
            var aggregateId = SaveTestAggregateWithoutCustomHeaders(_repo, 10);

            var streamName = string.Format("testAggregate-{0}", aggregateId);
            _connection.DeleteStream(streamName, 11);

            Assert.Throws<AggregateDeletedException>(() => _repo.GetById<TestAggregate>(aggregateId));
        }

        [Test]
        public void SavesCommitHeadersOnEachEvent()
        {
            var commitId = Guid.NewGuid();
            var aggregateToSave = new TestAggregate(Guid.NewGuid());
            aggregateToSave.ProduceEvents(20);
            _repo.Save(aggregateToSave, commitId, d => {
                d.Add("CustomHeader1", "CustomValue1");
                d.Add("CustomHeader2", "CustomValue2");
            });

            var read = _connection.ReadStreamEventsForward(string.Format("aggregate-{0}", aggregateToSave.Id), 1, 20, false);
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
        }
    }
}