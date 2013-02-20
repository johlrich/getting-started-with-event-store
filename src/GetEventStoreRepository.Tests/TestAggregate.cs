using System;
using CommonDomain.Core;

namespace GetEventStoreRepository.Tests
{
    public class TestAggregate : AggregateBase
    {
        public TestAggregate(Guid aggregateId) : this()
        {
            RaiseEvent(new TestAggregateCreated(aggregateId));
        }

        private TestAggregate()
        {
            Register<TestAggregateCreated>(e => Id = e.AggregateId);
            Register<WoftamEvent>(e => AppliedEventCount++);
        }

        public int AppliedEventCount { get; private set; }

        public void ProduceEvents(int count)
        {
            for (int i = 0; i < count; i++)
                RaiseEvent(new WoftamEvent("Woftam1-" + i, "Woftam2-" + i));
        }
    }
}