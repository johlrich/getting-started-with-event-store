using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CommonDomain;
using CommonDomain.Persistence;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GetEventStoreRepository
{
    public class GetEventStoreRepository : IRepository
    {
        private const string EventClrTypeHeader = "EventClrTypeName";
        private const string AggregateClrTypeHeader = "AggregateClrTypeName";
        private const string CommitIdHeader = "CommitId";
        private const int WritePageSize = 500;
        private const int ReadPageSize = 500;

        private readonly Func<Type, Guid, string> _aggregateIdToStreamName;

        private readonly EventStoreConnection _eventStoreConnection;
        private static readonly JsonSerializerSettings SerializerSettings;

        static GetEventStoreRepository()
        {
            SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        }

        public GetEventStoreRepository(EventStoreConnection eventStoreConnection)
            : this(eventStoreConnection, (t, g) => string.Format("{0}-{1}", char.ToLower(t.Name[0]) + t.Name.Substring(1), g))
        {
        }

        public GetEventStoreRepository(EventStoreConnection eventStoreConnection, Func<Type, Guid, string> aggregateIdToStreamName)
        {
            _eventStoreConnection = eventStoreConnection;
            _aggregateIdToStreamName = aggregateIdToStreamName;
        }

        public TAggregate GetById<TAggregate>(Guid id) where TAggregate : class, IAggregate
        {
            var streamName = _aggregateIdToStreamName(typeof(TAggregate), id);
            var aggregate = ConstructAggregate<TAggregate>();

            StreamEventsSlice currentSlice;
            var nextSliceStart = 1;
            do
            {
                currentSlice = _eventStoreConnection.ReadStreamEventsForward(streamName, nextSliceStart, ReadPageSize, false);
                nextSliceStart = currentSlice.NextEventNumber;

                foreach (var evnt in currentSlice.Events)
                    aggregate.ApplyEvent(DeserializeEvent(evnt.OriginalEvent.Metadata, evnt.OriginalEvent.Data));
            } while (!currentSlice.IsEndOfStream);

            return aggregate;
        }

        public TAggregate GetById<TAggregate>(Guid id, int version) where TAggregate : class, IAggregate
        {
            var streamName = _aggregateIdToStreamName(typeof(TAggregate), id);
            var aggregate = ConstructAggregate<TAggregate>();

            var sliceStart = 1; //Ignores $StreamCreated
            StreamEventsSlice currentSlice;
            do
            {
                var sliceCount = sliceStart + ReadPageSize <= version
                                    ? ReadPageSize
                                    : version - sliceStart + 1;

                currentSlice = _eventStoreConnection.ReadStreamEventsForward(streamName, sliceStart, sliceCount, false);
                sliceStart = currentSlice.NextEventNumber;

                foreach (var evnt in currentSlice.Events)
                    aggregate.ApplyEvent(DeserializeEvent(evnt.OriginalEvent.Metadata, evnt.OriginalEvent.Data));
            } while (version > currentSlice.NextEventNumber && !currentSlice.IsEndOfStream);

            return aggregate;
        }

        public object DeserializeEvent(byte[] metadata, byte[] data)
        {
            var eventClrTypeName = JObject.Parse(Encoding.UTF8.GetString(metadata)).Property(EventClrTypeHeader).Value;
            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data), Type.GetType((string)eventClrTypeName));
        }

        public void Save(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            var commitHeaders = new Dictionary<string, object>
            {
                {CommitIdHeader, commitId},
                {AggregateClrTypeHeader, aggregate.GetType().AssemblyQualifiedName}
            };
            updateHeaders(commitHeaders);

            var streamName = _aggregateIdToStreamName(aggregate.GetType(), aggregate.Id);
            var newEvents = aggregate.GetUncommittedEvents().Cast<object>().ToList();
            var originalVersion = aggregate.Version - newEvents.Count;
            var expectedVersion = originalVersion == 0 ? -1 : originalVersion;
            var preparedEvents = PrepareEvents(newEvents, commitHeaders).ToList();

            if (preparedEvents.Count < WritePageSize)
            {
                _eventStoreConnection.AppendToStream(streamName, expectedVersion, preparedEvents);
            }
            else
            {
                var transaction = _eventStoreConnection.StartTransaction(streamName, expectedVersion);

                var position = 0;
                while (position < preparedEvents.Count)
                {
                    var pageEvents = preparedEvents.Skip(position).Take(WritePageSize);
                    transaction.Write(pageEvents);
                    position += WritePageSize;
                }

                transaction.Commit();
            }

            aggregate.ClearUncommittedEvents();
        }

        private static TAggregate ConstructAggregate<TAggregate>()
        {
            return (TAggregate)Activator.CreateInstance(typeof(TAggregate), true);
        }

        private static IEnumerable<EventData> PrepareEvents(IEnumerable<object> events, IDictionary<string, object> commitHeaders)
        {
            return events.Select(e => CreateEventData(Guid.NewGuid(), e, commitHeaders));
        }

        private static EventData CreateEventData(Guid eventId, object evnt, IDictionary<string, object> headers)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(evnt, SerializerSettings));

            var eventHeaders = new Dictionary<string, object>(headers)
            {
                {
                    EventClrTypeHeader, evnt.GetType().AssemblyQualifiedName
                }
            };
            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(eventHeaders, SerializerSettings));
            var typeName = evnt.GetType().Name;

            return new EventData(eventId, typeName, true, data, metadata);
        }
    }
}