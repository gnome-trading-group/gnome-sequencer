package group.gnometrading.sequencer;

import org.agrona.concurrent.UnsafeBuffer;

@FunctionalInterface
public interface SequencedEventHandler {

    void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length) throws Exception;
}
