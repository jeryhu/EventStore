using System;

namespace EventStore.Projections.Core.Services.Processing
{
    public interface IEmittedStreamManager
    {
        void TrackEmittedStream(EmittedEvent[] emittedEvents);
        void DeleteEmittedStreams(Action onEmittedStreamsDeleted);
    }
}
