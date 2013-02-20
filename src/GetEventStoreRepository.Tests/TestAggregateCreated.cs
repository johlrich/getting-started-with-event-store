using System;

namespace GetEventStoreRepository.Tests
{
    public class TestAggregateCreated
    {
        public TestAggregateCreated(Guid aggregateId)
        {
            AggregateId = aggregateId;
        }

        public Guid AggregateId { get; private set; }
    }
}