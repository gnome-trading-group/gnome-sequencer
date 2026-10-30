package group.gnometrading.sequencer;

import org.agrona.concurrent.UnsafeBuffer;

/** A {@link SequencedEventHandler} that can refuse an event, to be handed it again on the next poll. */
@FunctionalInterface
public interface ControlledSequencedEventHandler {

    /** False holds the event: the poll stops there, and the next one starts with it. */
    boolean onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length) throws Exception;
}
